from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.safari_count") > 9)
    .select(F.col("c.age"), F.col("t.safari_count"), F.col("t.browser_values"))
    .orderBy(F.col("age").desc())
)
