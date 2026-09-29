from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14400, "high").when(F.col("c.credit_limit") >= 2700, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("s.device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
        F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"),
    )
)
