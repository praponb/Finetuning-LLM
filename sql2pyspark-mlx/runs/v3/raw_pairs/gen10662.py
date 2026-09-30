from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").isNotNull())
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("s.device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
    .orderBy(F.col("country").desc())
)
