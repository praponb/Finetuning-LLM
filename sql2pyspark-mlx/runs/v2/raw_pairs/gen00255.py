from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"), F.col("c.is_active"))
    .agg(
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
        F.sum(F.when(F.col("s.browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
    )
    .filter(F.col("firefox_count") >= 68)
    .orderBy("firefox_count", "segment", "is_active")
    .limit(3)
)
