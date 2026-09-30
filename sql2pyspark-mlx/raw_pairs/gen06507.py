from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") >= 53)
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("s.country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
    )
    .orderBy("uk_count", "segment")
)
