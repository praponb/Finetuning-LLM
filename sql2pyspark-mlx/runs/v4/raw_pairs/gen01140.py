from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.total_count") >= 7)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("t.total_count"),
        F.col("t.browser_values"),
    )
    .orderBy(F.col("total_count").desc())
)
