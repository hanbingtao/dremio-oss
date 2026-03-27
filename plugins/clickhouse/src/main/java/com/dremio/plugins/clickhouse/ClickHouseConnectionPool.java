/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.plugins.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection pool for ClickHouse JDBC connections. */
public class ClickHouseConnectionPool {

  private static final Logger logger = LoggerFactory.getLogger(ClickHouseConnectionPool.class);

  private final String jdbcUrl;
  private final String database;
  private final String username;
  private final String password;
  private final int maxConnections;
  private final ConcurrentHashMap<String, ConnectionWrapper> connections;
  private final AtomicInteger connectionCounter;

  public ClickHouseConnectionPool(
      String host,
      int port,
      String database,
      String username,
      String password,
      boolean useSsl,
      int maxConnections) {
    this.jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
    this.database = database;
    this.username = username;
    this.password = password;
    this.maxConnections = maxConnections;
    this.connections = new ConcurrentHashMap<>();
    this.connectionCounter = new AtomicInteger(0);

    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
    } catch (ClassNotFoundException e) {
      logger.error("ClickHouse JDBC driver not found", e);
    }
  }

  public Connection getConnection() throws SQLException {
    // Simple connection creation (in production, implement proper pooling)
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);
    props.setProperty("connect_timeout", "30000");
    props.setProperty("socket_timeout", "60000");
    props.setProperty("compress", "0");

    return DriverManager.getConnection(jdbcUrl, props);
  }

  public void closeConnection(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        logger.error("Error closing connection", e);
      }
    }
  }

  /** Test connection to ClickHouse. */
  public boolean testConnection() {
    Connection conn = null;
    try {
      conn = getConnection();
      return conn != null && !conn.isClosed();
    } catch (SQLException e) {
      logger.error("Connection test failed: {}", e.getMessage());
      return false;
    } finally {
      closeConnection(conn);
    }
  }

  /** Get list of tables from ClickHouse. */
  public List<TablePath> getTables() throws SQLException {
    List<TablePath> tables = new ArrayList<>();
    Connection conn = null;
    try {
      conn = getConnection();
      String sql = buildTableDiscoverySql();
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        bindDiscoveryParameters(stmt);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            tables.add(new TablePath(rs.getString("database"), rs.getString("name")));
          }
        }
      }
    } finally {
      closeConnection(conn);
    }
    logger.info("Discovered {} ClickHouse tables for source database setting {}", tables.size(), database);
    return tables;
  }

  /** Get column metadata for a table. */
  public List<ColumnMetaData> getColumns(String databaseName, String tableName) throws SQLException {
    List<ColumnMetaData> columns = new ArrayList<>();
    Connection conn = null;
    try {
      conn = getConnection();
      String sql =
          "SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, databaseName);
        stmt.setString(2, tableName);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String typeName = rs.getString("type");
            columns.add(
                new ColumnMetaData(
                    rs.getString("name"), typeName, toSqlType(typeName), 0));
          }
        }
      }
    } finally {
      closeConnection(conn);
    }
    return columns;
  }

  private String buildTableDiscoverySql() {
    if (shouldDiscoverAllDatabases()) {
      return "SELECT database, name FROM system.tables "
          + "WHERE is_temporary = 0 "
          + "AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') "
          + "ORDER BY database, name";
    }

    return "SELECT database, name FROM system.tables "
        + "WHERE database = ? AND is_temporary = 0 "
        + "ORDER BY database, name";
  }

  private void bindDiscoveryParameters(PreparedStatement stmt) throws SQLException {
    if (!shouldDiscoverAllDatabases()) {
      stmt.setString(1, database);
    }
  }

  private boolean shouldDiscoverAllDatabases() {
    return database == null || database.isBlank() || "default".equalsIgnoreCase(database);
  }

  private int toSqlType(String typeName) {
    if (typeName == null) {
      return Types.VARCHAR;
    }

    String normalized = typeName.toUpperCase();
    if (normalized.startsWith("INT") || normalized.startsWith("UINT")) {
      return Types.BIGINT;
    } else if (normalized.startsWith("FLOAT")) {
      return Types.FLOAT;
    } else if (normalized.startsWith("DECIMAL")) {
      return Types.DECIMAL;
    } else if (normalized.startsWith("DATE")) {
      return Types.TIMESTAMP;
    } else if (normalized.startsWith("BOOL")) {
      return Types.BOOLEAN;
    }

    return Types.VARCHAR;
  }

  /** Close all connections. */
  public void close() {
    for (ConnectionWrapper wrapper : connections.values()) {
      closeConnection(wrapper.connection);
    }
    connections.clear();
  }

  /** Wrapper for connection with metadata. */
  private static class ConnectionWrapper {
    final Connection connection;
    final long createdAt;
    final String threadName;

    ConnectionWrapper(Connection connection) {
      this.connection = connection;
      this.createdAt = System.currentTimeMillis();
      this.threadName = Thread.currentThread().getName();
    }
  }

  /** Column metadata holder. */
  public static class ColumnMetaData {
    private final String name;
    private final String typeName;
    private final int dataType;
    private final int columnSize;

    public ColumnMetaData(String name, String typeName, int dataType, int columnSize) {
      this.name = name;
      this.typeName = typeName;
      this.dataType = dataType;
      this.columnSize = columnSize;
    }

    public String getName() {
      return name;
    }

    public String getTypeName() {
      return typeName;
    }

    public int getDataType() {
      return dataType;
    }

    public int getColumnSize() {
      return columnSize;
    }
  }

  /** Table path holder. */
  public static class TablePath {
    private final String databaseName;
    private final String tableName;

    public TablePath(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }
  }
}
