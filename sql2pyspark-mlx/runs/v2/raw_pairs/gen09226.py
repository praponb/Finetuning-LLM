from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.segment"), F.col("cs.total_count"), F.col("cs.browser_values"))
)
