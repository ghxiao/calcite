/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.examples.foodmart.java;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Example of using Calcite via JDBC.
 *
 * <p>Schema is specified programmatically.</p>
 */
public class JdbcRelBuilderExample {
    public static void main(String[] args) throws Exception {
        new JdbcRelBuilderExample().run();
    }

    public void run() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("hr", new ReflectiveSchema(new Hr()));
        rootSchema.add("foodmart", new ReflectiveSchema(new Foodmart()));

//        DatabaseMetaData md = calciteConnection.getMetaData();
//        ResultSet tables = md.getTables(null, null, "%", null);
//
//        printResultSet(tables);

        final FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        final RelBuilder b = RelBuilder.create(calciteFrameworkConfig);


//        b
//                .scan("hr", "emps")
//                .scan("foodmart", "sales_fact_1997")
//                .join(JoinRelType.LEFT,
//                        b.call(SqlStdOperatorTable.EQUALS,
//                        b.field(2, 0, "empid"),
//                        b.field(2, 1, "cust_id")))
//                .project(b.alias(b.field("empid"), "e_id"),
//                        b.field("name"),
//                        b.field("prod_id"))
//                .project(b.field("e_id"),
//                        b.alias(b.field("e_id"), "e_id2"),
//                        b.field("name"))
////                .project(b.alias(b.field("empid"), "e_id"),
////                        b.field("name"),R
////                        b.field("prod_id"))
//        ;

        b
                .scan("hr", "emps")
                .project(b.alias(b.field("empid"), "id"))
                .scan("foodmart", "sales_fact_1997")
                .project(b.alias(b.field("cust_id"), "id"))
                .union(true)
//                .project(ImmutableList.of(b.field("id")),
//                        ImmutableList.of("id1"), true) // <- renaming is ignored in the generated SQL
//                .project(ImmutableList.of(b.field("id"),b.field("id")),
//                        ImmutableList.of("id1", "id2")) // <- fails
                .project(ImmutableList.of(b.field("id"),b.field("id")),
                        ImmutableList.of("id1", "id")) // <- fails
        ;

        SqlDialect dialect = SqlDialect.CALCITE;
        final RelNode node = b.build();
        final RelToSqlConverter converter = new RelToSqlConverter(dialect);
        final SqlNode sqlNode = converter.visitChild(0, node).asStatement();
        final String sql = sqlNode.toSqlString(dialect).getSql();

        System.out.println(sql);
        System.out.println();

        Statement statement = connection.createStatement();
//        final String sql1 = "SELECT *\n"
//                + "FROM \"foodmart\".\"sales_fact_1997\" AS s\n"
//                + "JOIN \"hr\".\"emps\" AS e\n"
//                + "ON e.\"empid\" = s.\"cust_id\"";
        ResultSet resultSet = statement.executeQuery(sql);
        printResultSet(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    private void printResultSet(ResultSet resultSet) throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
    }

    /**
     * Object that will be used via reflection to create the "hr" schema.
     */
    public static class Hr {
        public final Employee[] emps = {
                new Employee(100, "Bill"),
                new Employee(200, "Eric"),
                new Employee(150, "Sebastian"),
        };
    }

    /**
     * Object that will be used via reflection to create the "emps" table.
     */
    public static class Employee {
        public final Integer empid;
        public final String name;

        public Employee(Integer empid, String name) {
            this.empid = empid;
            this.name = name;
        }
    }

    /**
     * Object that will be used via reflection to create the "foodmart"
     * schema.
     */
    public static class Foodmart {
        public final SalesFact[] sales_fact_1997 = {
                new SalesFact(100, 10),
                new SalesFact(150, 20),
        };
    }


    /**
     * Object that will be used via reflection to create the
     * "sales_fact_1997" fact table.
     */
    public static class SalesFact {
        public final Integer cust_id;
        public final Integer prod_id;

        public SalesFact(Integer cust_id, Integer prod_id) {
            this.cust_id = cust_id;
            this.prod_id = prod_id;
        }
    }
}

// End JdbcRelBuilderExample.java
