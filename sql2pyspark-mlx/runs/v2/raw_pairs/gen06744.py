from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device") == "tablet")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("s.browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
    .orderBy("segment")
    .limit(5)
)
