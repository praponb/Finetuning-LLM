"""Hand-written SQL -> PySpark pairs (the v1 curated set, ported to the v2 conventions).

These cover features the compositional generator produces rarely or never
(CROSS JOIN, INTERSECT/EXCEPT across tables, NULLS LAST, POWER, SPLIT, ...)
and add some human-style variety. All pairs use tables from schemas.py so they
can be execution-checked. They always go to the training split.

Usage:  python handwritten_pairs.py --out raw_pairs
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def P(sql: str, body: str, order=()):
    """body = PySpark statements without the import lines; imports are added here."""
    imports = ["from pyspark.sql import functions as F"]
    if "Window." in body:
        imports.append("from pyspark.sql.window import Window")
    return sql.strip(), "\n".join(imports) + "\n\n" + body.strip(), list(order)


PAIRS = [
    # --- 1. Basic select & filtering ---
    P("SELECT id, name, email FROM customers;",
      'result = df_customers.select("id", "name", "email")'),
    P("SELECT id AS customer_id, name AS customer_name FROM customers;",
      'result = df_customers.select(F.col("id").alias("customer_id"), F.col("name").alias("customer_name"))'),
    P("SELECT * FROM employees WHERE salary > 50000;",
      'result = df_employees.filter(F.col("salary") > 50000)'),
    P("SELECT * FROM orders WHERE status = 'COMPLETED' AND amount >= 100;",
      'result = df_orders.filter((F.col("status") == "COMPLETED") & (F.col("amount") >= 100))'),
    P("SELECT * FROM orders WHERE channel = 'web' OR channel = 'mobile';",
      'result = df_orders.filter((F.col("channel") == "web") | (F.col("channel") == "mobile"))'),
    P("SELECT * FROM products WHERE category IN ('Electronics', 'Books', 'Sports');",
      'result = df_products.filter(F.col("category").isin(["Electronics", "Books", "Sports"]))'),
    P("SELECT * FROM products WHERE category NOT IN ('Grocery', 'Fashion');",
      'result = df_products.filter(~F.col("category").isin(["Grocery", "Fashion"]))'),
    P("SELECT * FROM transactions WHERE amount BETWEEN 50 AND 2000;",
      'result = df_transactions.filter(F.col("amount").between(50, 2000))'),
    P("SELECT * FROM customers WHERE city IS NULL;",
      'result = df_customers.filter(F.col("city").isNull())'),
    P("SELECT * FROM customers WHERE email IS NOT NULL;",
      'result = df_customers.filter(F.col("email").isNotNull())'),
    # --- 2. Math & column operations ---
    P("SELECT id, quantity * unit_price AS line_total FROM order_items;",
      'result = df_order_items.select("id", (F.col("quantity") * F.col("unit_price")).alias("line_total"))'),
    P("SELECT id, ROUND(rating, 1) AS rounded_rating FROM suppliers;",
      'result = df_suppliers.select("id", F.round("rating", 1).alias("rounded_rating"))'),
    P("SELECT id, ABS(balance) AS abs_balance FROM accounts;",
      'result = df_accounts.select("id", F.abs("balance").alias("abs_balance"))'),
    P("SELECT id, sqft % 2 AS is_odd FROM stores;",
      'result = df_stores.select("id", (F.col("sqft") % 2).alias("is_odd"))'),
    P("SELECT id, POWER(rating, 2) AS rating_squared FROM suppliers;",
      'result = df_suppliers.select("id", F.pow("rating", 2).alias("rating_squared"))'),
    P("SELECT id, CEIL(price) AS price_ceil, FLOOR(price) AS price_floor FROM products;",
      'result = df_products.select("id", F.ceil("price").alias("price_ceil"), F.floor("price").alias("price_floor"))'),
    P("SELECT id, CAST(price AS INT) AS price_int FROM products;",
      'result = df_products.select("id", F.col("price").cast("int").alias("price_int"))'),
    P("SELECT id, CAST(age AS DOUBLE) AS age_double FROM customers;",
      'result = df_customers.select("id", F.col("age").cast("double").alias("age_double"))'),
    P("SELECT id, CAST(signup_date AS STRING) AS signup_str FROM customers;",
      'result = df_customers.select("id", F.col("signup_date").cast("string").alias("signup_str"))'),
    P("SELECT *, salary * 1.1 AS new_salary FROM employees;",
      'result = df_employees.withColumn("new_salary", F.col("salary") * 1.1)'),
    # --- 3. Aggregations & GROUP BY ---
    P("SELECT COUNT(*) AS total_rows FROM web_events;",
      'result = df_web_events.agg(F.count("*").alias("total_rows"))'),
    P("SELECT COUNT(DISTINCT customer_id) AS active_customers FROM sessions;",
      'result = df_sessions.agg(F.countDistinct("customer_id").alias("active_customers"))'),
    P("SELECT dept_id, SUM(salary) AS total_payroll FROM employees GROUP BY dept_id;",
      'result = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("total_payroll"))'),
    P("SELECT country, AVG(age) AS avg_age FROM customers GROUP BY country;",
      'result = df_customers.groupBy("country").agg(F.avg("age").alias("avg_age"))'),
    P("SELECT category, MIN(price) AS min_price, MAX(price) AS max_price FROM products GROUP BY category;",
      """result = (
    df_products
    .groupBy("category")
    .agg(F.min("price").alias("min_price"), F.max("price").alias("max_price"))
)"""),
    P("""SELECT store_id, product_id, SUM(units) AS total_units, AVG(revenue) AS avg_revenue
FROM store_sales
GROUP BY store_id, product_id;""",
      """result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
)"""),
    P("SELECT YEAR(order_date) AS yr, MONTH(order_date) AS mo, COUNT(*) AS order_count FROM orders "
      "GROUP BY YEAR(order_date), MONTH(order_date);",
      """result = (
    df_orders
    .groupBy(F.year("order_date").alias("yr"), F.month("order_date").alias("mo"))
    .agg(F.count("*").alias("order_count"))
)"""),
    P("SELECT dept_id, COUNT(*) AS emp_count FROM employees GROUP BY dept_id HAVING COUNT(*) > 5;",
      """result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("emp_count"))
    .filter(F.col("emp_count") > 5)
)"""),
    P("SELECT store_id, SUM(revenue) AS total_rev FROM store_sales GROUP BY store_id HAVING SUM(revenue) >= 10000;",
      """result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_rev"))
    .filter(F.col("total_rev") >= 10000)
)"""),
    P("SELECT device, COUNT(DISTINCT customer_id) AS unique_visitors FROM sessions GROUP BY device;",
      'result = df_sessions.groupBy("device").agg(F.countDistinct("customer_id").alias("unique_visitors"))'),
    # --- 4. Joins ---
    P("SELECT c.id, c.name, o.id AS order_id FROM customers c INNER JOIN orders o ON c.id = o.customer_id;",
      """result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("o.id").alias("order_id"))
)"""),
    P("""SELECT i.store_id, i.product_id, i.quantity, s.units
FROM inventory i
JOIN store_sales s ON i.store_id = s.store_id AND i.product_id = s.product_id;""",
      """result = (
    df_inventory.alias("i")
    .join(
        df_store_sales.alias("s"),
        (F.col("i.store_id") == F.col("s.store_id")) & (F.col("i.product_id") == F.col("s.product_id")),
        "inner",
    )
    .select(F.col("i.store_id"), F.col("i.product_id"), F.col("i.quantity"), F.col("s.units"))
)"""),
    P("SELECT c.id, c.name, o.order_date FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;",
      """result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.name"), F.col("o.order_date"))
)"""),
    P("SELECT p.product_name, oi.quantity FROM products p RIGHT JOIN order_items oi ON p.id = oi.product_id;",
      """result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "right")
    .select(F.col("p.product_name"), F.col("oi.quantity"))
)"""),
    P("SELECT a.id AS account_id, c.id AS customer_id FROM accounts a FULL OUTER JOIN customers c ON a.customer_id = c.id;",
      """result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .select(F.col("a.id").alias("account_id"), F.col("c.id").alias("customer_id"))
)"""),
    P("SELECT * FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);",
      """result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
)"""),
    P("SELECT * FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);",
      """result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
)"""),
    P("SELECT s.region, p.category FROM stores s CROSS JOIN products p;",
      """result = (
    df_stores.alias("s")
    .crossJoin(df_products.alias("p"))
    .select(F.col("s.region"), F.col("p.category"))
)"""),
    P("SELECT e.full_name AS emp_name, m.full_name AS manager_name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;",
      """result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(F.col("e.full_name").alias("emp_name"), F.col("m.full_name").alias("manager_name"))
)"""),
    P("SELECT o.id, o.amount, c.country FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'TH';",
      """result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "TH")
    .select(F.col("o.id"), F.col("o.amount"), F.col("c.country"))
)"""),
    # --- 5. Window functions ---
    P("SELECT id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rank_num FROM employees;",
      """w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = df_employees.select("id", "dept_id", "salary", F.row_number().over(w).alias("rank_num"))"""),
    P("SELECT id, category, price, RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank FROM products;",
      """w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = df_products.select("id", "category", "price", F.rank().over(w).alias("price_rank"))"""),
    P("SELECT id, brand, stock_qty, DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty DESC) AS stock_rank FROM products;",
      """w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = df_products.select("id", "brand", "stock_qty", F.dense_rank().over(w).alias("stock_rank"))"""),
    P("""SELECT account_id, txn_time, amount,
       SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS running_total
FROM transactions;""",
      """w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = df_transactions.select("account_id", "txn_time", "amount", F.sum("amount").over(w).alias("running_total"))"""),
    P("""SELECT id, sale_date, revenue,
       AVG(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7
FROM store_sales;""",
      """w = Window.orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = df_store_sales.select("id", "sale_date", "revenue", F.avg("revenue").over(w).alias("moving_avg_7"))"""),
    P("SELECT session_id, event_time, LAG(page, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_page FROM web_events;",
      """w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = df_web_events.select("session_id", "event_time", F.lag("page", 1).over(w).alias("prev_page"))"""),
    P("SELECT id, order_date, LEAD(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_order_date FROM orders;",
      """w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = df_orders.select("id", "order_date", F.lead("order_date", 1).over(w).alias("next_order_date"))"""),
    P("SELECT id, dept_id, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS max_salary FROM employees;",
      """w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = df_employees.select("id", "dept_id", "salary", F.first("salary").over(w).alias("max_salary"))"""),
    P("""SELECT id, dept_id, salary,
       LAST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_salary
FROM employees;""",
      """w = (
    Window.partitionBy("dept_id")
    .orderBy("salary")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

result = df_employees.select("id", "dept_id", "salary", F.last("salary").over(w).alias("highest_salary"))"""),
    P("SELECT id, credit_limit, NTILE(4) OVER (ORDER BY credit_limit DESC, id) AS quartile FROM customers;",
      """w = Window.orderBy(F.col("credit_limit").desc(), "id")

result = df_customers.select("id", "credit_limit", F.ntile(4).over(w).alias("quartile"))"""),
    # --- 6. Conditional logic ---
    P("SELECT id, CASE WHEN age >= 30 THEN 'Senior' ELSE 'Junior' END AS age_group FROM customers;",
      'result = df_customers.select("id", F.when(F.col("age") >= 30, "Senior").otherwise("Junior").alias("age_group"))'),
    P("""SELECT id,
       CASE WHEN rating >= 4.5 THEN 'A'
            WHEN rating >= 3.5 THEN 'B'
            WHEN rating >= 2.5 THEN 'C'
            ELSE 'D' END AS grade
FROM suppliers;""",
      """result = df_suppliers.select(
    "id",
    F.when(F.col("rating") >= 4.5, "A")
    .when(F.col("rating") >= 3.5, "B")
    .when(F.col("rating") >= 2.5, "C")
    .otherwise("D")
    .alias("grade"),
)"""),
    P("SELECT id, COALESCE(city, country, 'unknown') AS location FROM customers;",
      'result = df_customers.select("id", F.coalesce("city", "country", F.lit("unknown")).alias("location"))'),
    P("SELECT id, NULLIF(discount, 0) AS discount_nonzero FROM orders;",
      'result = df_orders.select("id", F.nullif("discount", F.lit(0)).alias("discount_nonzero"))'),
    P("""SELECT id,
       CASE WHEN segment = 'Corporate' AND is_active = TRUE THEN 0.20
            WHEN segment = 'SMB' THEN 0.10
            ELSE 0.0 END AS discount_rate
FROM customers;""",
      """result = df_customers.select(
    "id",
    F.when((F.col("segment") == "Corporate") & F.col("is_active"), 0.20)
    .when(F.col("segment") == "SMB", 0.10)
    .otherwise(0.0)
    .alias("discount_rate"),
)"""),
    P("SELECT dept_id, SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) AS remote_count FROM employees GROUP BY dept_id;",
      """result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum(F.when(F.col("is_remote"), 1).otherwise(0)).alias("remote_count"))
)"""),
    P("SELECT order_date, SUM(CASE WHEN status = 'RETURNED' THEN amount ELSE 0 END) AS total_returned FROM orders GROUP BY order_date;",
      """result = (
    df_orders
    .groupBy("order_date")
    .agg(F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("total_returned"))
)"""),
    P("SELECT id, IF(is_flagged, 'Review', 'OK') AS review_status FROM transactions;",
      'result = df_transactions.select("id", F.when(F.col("is_flagged"), "Review").otherwise("OK").alias("review_status"))'),
    P("SELECT id, COALESCE(discount, 0) AS final_discount FROM orders;",
      'result = df_orders.select("id", F.coalesce("discount", F.lit(0)).alias("final_discount"))'),
    P("SELECT id, CASE WHEN country IN ('US', 'UK', 'DE') THEN 'West' ELSE 'Asia' END AS market FROM customers;",
      'result = df_customers.select("id", F.when(F.col("country").isin(["US", "UK", "DE"]), "West").otherwise("Asia").alias("market"))'),
    # --- 7. Dates & times ---
    P("SELECT id, DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup FROM customers;",
      'result = df_customers.select("id", F.datediff(F.current_date(), "signup_date").alias("days_since_signup"))'),
    P("SELECT id, DATE_ADD(signup_date, 30) AS trial_end FROM customers;",
      'result = df_customers.select("id", F.date_add("signup_date", 30).alias("trial_end"))'),
    P("SELECT id, DATE_SUB(order_date, 7) AS week_before FROM orders;",
      'result = df_orders.select("id", F.date_sub("order_date", 7).alias("week_before"))'),
    P("SELECT id, DATEDIFF(delivered_date, shipped_date) AS transit_days FROM shipments;",
      'result = df_shipments.select("id", F.datediff("delivered_date", "shipped_date").alias("transit_days"))'),
    P("SELECT id, YEAR(order_date) AS yr, MONTH(order_date) AS mo, DAY(order_date) AS dy FROM orders;",
      """result = df_orders.select(
    "id",
    F.year("order_date").alias("yr"),
    F.month("order_date").alias("mo"),
    F.dayofmonth("order_date").alias("dy"),
)"""),
    P("SELECT id, DATE_TRUNC('month', txn_time) AS txn_month FROM transactions;",
      'result = df_transactions.select("id", F.date_trunc("month", "txn_time").alias("txn_month"))'),
    P("SELECT id, TO_DATE(paid_at) AS paid_date FROM payments;",
      'result = df_payments.select("id", F.to_date("paid_at").alias("paid_date"))'),
    P("SELECT id, DATE_FORMAT(opened_date, 'yyyy/MM') AS period FROM accounts;",
      'result = df_accounts.select("id", F.date_format("opened_date", "yyyy/MM").alias("period"))'),
    P("SELECT id, ADD_MONTHS(start_date, 12) AS renewal_date FROM subscriptions;",
      'result = df_subscriptions.select("id", F.add_months("start_date", 12).alias("renewal_date"))'),
    P("SELECT id, HOUR(event_time) AS hr, MINUTE(event_time) AS mn, SECOND(event_time) AS sc FROM web_events;",
      """result = df_web_events.select(
    "id",
    F.hour("event_time").alias("hr"),
    F.minute("event_time").alias("mn"),
    F.second("event_time").alias("sc"),
)"""),
    # --- 8. Strings ---
    P("SELECT id, CONCAT(full_name, ' <', email, '>') AS contact FROM employees;",
      'result = df_employees.select("id", F.concat("full_name", F.lit(" <"), "email", F.lit(">")).alias("contact"))'),
    P("SELECT id, CONCAT_WS('-', country, city, segment) AS customer_key FROM customers;",
      'result = df_customers.select("id", F.concat_ws("-", "country", "city", "segment").alias("customer_key"))'),
    P("SELECT id, SUBSTRING(product_name, 1, 4) AS prefix FROM products;",
      'result = df_products.select("id", F.substring("product_name", 1, 4).alias("prefix"))'),
    P("SELECT id, LENGTH(TRIM(name)) AS name_len FROM customers;",
      'result = df_customers.select("id", F.length(F.trim("name")).alias("name_len"))'),
    P("SELECT id, UPPER(brand) AS brand_upper, LOWER(product_name) AS name_lower FROM products;",
      'result = df_products.select("id", F.upper("brand").alias("brand_upper"), F.lower("product_name").alias("name_lower"))'),
    P("SELECT id, TRIM(name) AS clean_name FROM customers;",
      'result = df_customers.select("id", F.trim("name").alias("clean_name"))'),
    P("SELECT id, REPLACE(product_name, '-', ' ') AS clean_name FROM products;",
      'result = df_products.select("id", F.regexp_replace("product_name", "-", " ").alias("clean_name"))'),
    P("SELECT * FROM products WHERE product_name LIKE '%Pro%';",
      'result = df_products.filter(F.col("product_name").like("%Pro%"))'),
    P("SELECT id, REGEXP_EXTRACT(email, '@([a-z.]+)', 1) AS domain FROM customers;",
      'result = df_customers.select("id", F.regexp_extract("email", "@([a-z.]+)", 1).alias("domain"))'),
    P("SELECT id, SPLIT(email, '@') AS email_parts FROM employees;",
      'result = df_employees.select("id", F.split("email", "@").alias("email_parts"))'),
    # --- 9. Ordering, limiting, dedup, set operations ---
    P("SELECT * FROM suppliers ORDER BY rating ASC;",
      'result = df_suppliers.orderBy(F.col("rating").asc())', ["rating"]),
    P("SELECT * FROM orders ORDER BY amount DESC, order_date ASC;",
      'result = df_orders.orderBy(F.col("amount").desc(), F.col("order_date").asc())', ["amount", "order_date"]),
    P("SELECT * FROM customers ORDER BY credit_limit DESC NULLS LAST;",
      'result = df_customers.orderBy(F.col("credit_limit").desc_nulls_last())', ["credit_limit"]),
    P("SELECT * FROM orders ORDER BY order_date DESC, id DESC LIMIT 10;",
      'result = df_orders.orderBy(F.col("order_date").desc(), F.col("id").desc()).limit(10)', ["order_date", "id"]),
    P("SELECT DISTINCT category FROM products;",
      'result = df_products.select("category").distinct()'),
    P("SELECT DISTINCT customer_id, device FROM sessions;",
      'result = df_sessions.select("customer_id", "device").distinct()'),
    P("SELECT id, amount FROM orders WHERE channel = 'web' UNION ALL SELECT id, amount FROM payments WHERE status = 'FAILED';",
      """result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("id", "amount")
    .unionByName(df_payments.filter(F.col("status") == "FAILED").select("id", "amount"))
)"""),
    P("SELECT city FROM customers UNION SELECT city FROM stores;",
      'result = df_customers.select("city").unionByName(df_stores.select("city")).distinct()'),
    P("SELECT country FROM customers INTERSECT SELECT country FROM suppliers;",
      'result = df_customers.select("country").intersect(df_suppliers.select("country"))'),
    P("SELECT id FROM customers EXCEPT SELECT customer_id FROM orders;",
      'result = df_customers.select("id").subtract(df_orders.select("customer_id"))'),
    # --- 10. Subqueries, CTEs & advanced ---
    P("SELECT * FROM (SELECT id, amount FROM orders WHERE amount > 500) sub WHERE id < 20;",
      """result = (
    df_orders
    .filter(F.col("amount") > 500)
    .select("id", "amount")
    .filter(F.col("id") < 20)
)"""),
    P("SELECT id, name FROM customers WHERE id IN (SELECT customer_id FROM subscriptions WHERE plan = 'premium');",
      """result = (
    df_customers.alias("c")
    .join(
        df_subscriptions.filter(F.col("plan") == "premium").alias("s"),
        F.col("c.id") == F.col("s.customer_id"),
        "left_semi",
    )
    .select(F.col("c.id"), F.col("c.name"))
)"""),
    P("""WITH high_value AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 2000
)
SELECT * FROM high_value;""",
      """high_value = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total"))
    .filter(F.col("total") > 2000)
)

result = high_value"""),
    P("""WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn
    FROM employees
)
SELECT * FROM ranked WHERE rn = 1;""",
      """w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)"""),
    P("SELECT id, EXPLODE(tags) AS tag FROM products;",
      'result = df_products.select("id", F.explode("tags").alias("tag"))'),
    P("SELECT id, ARRAY_CONTAINS(tags, 'sale') AS on_sale FROM products;",
      'result = df_products.select("id", F.array_contains("tags", "sale").alias("on_sale"))'),
    P("SELECT region, city, SUM(sqft) AS total_sqft FROM stores GROUP BY ROLLUP(region, city);",
      'result = df_stores.rollup("region", "city").agg(F.sum("sqft").alias("total_sqft"))'),
    P("SELECT channel, status, COUNT(*) AS n FROM orders GROUP BY CUBE(channel, status);",
      'result = df_orders.cube("channel", "status").agg(F.count("*").alias("n"))'),
    P("""SELECT * FROM (
    SELECT segment, country, credit_limit FROM customers
) PIVOT (
    SUM(credit_limit) FOR country IN ('TH', 'US', 'SG')
);""",
      """result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["TH", "US", "SG"])
    .agg(F.sum("credit_limit"))
)"""),
    P("SELECT session_id, SORT_ARRAY(COLLECT_LIST(page)) AS pages FROM web_events GROUP BY session_id;",
      'result = df_web_events.groupBy("session_id").agg(F.sort_array(F.collect_list("page")).alias("pages"))'),
    # --- 11. Short everyday queries ---
    P("SELECT id, name FROM customers WHERE segment = 'Consumer';",
      'result = df_customers.filter(F.col("segment") == "Consumer").select("id", "name")'),
    P("SELECT dept_id, COUNT(*) AS total FROM employees GROUP BY dept_id;",
      'result = df_employees.groupBy("dept_id").agg(F.count("*").alias("total"))'),
    P("SELECT * FROM orders ORDER BY amount DESC LIMIT 10;",
      'result = df_orders.orderBy(F.col("amount").desc()).limit(10)', ["amount"]),
    P("SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id;",
      """result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(F.col("c.name"), F.col("o.amount"))
)"""),
    P("SELECT COUNT(DISTINCT country) AS country_count FROM suppliers;",
      'result = df_suppliers.agg(F.countDistinct("country").alias("country_count"))'),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="raw_pairs")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("hw*.*"):
        f.unlink()
    with open(out / "meta_handwritten.jsonl", "w", encoding="utf-8") as meta:
        for i, (sql, code, order) in enumerate(PAIRS, 1):
            pid = f"hw{i:03d}"
            (out / f"{pid}.sql").write_text(sql + "\n", encoding="utf-8")
            (out / f"{pid}.py").write_text(code + "\n", encoding="utf-8")
            meta.write(json.dumps({"id": pid, "family": "handwritten", "features": ["handwritten"],
                                   "order_cols": order}) + "\n")
    print(f"wrote {len(PAIRS)} hand-written pairs to {out}/")


if __name__ == "__main__":
    main()
