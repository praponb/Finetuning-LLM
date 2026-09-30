from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("t.browser_values"),
    )
    .orderBy(F.col("age").desc())
)
