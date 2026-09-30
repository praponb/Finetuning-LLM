from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("cs.browser_values"),
        F.col("cs.jp_count"),
    )
)
