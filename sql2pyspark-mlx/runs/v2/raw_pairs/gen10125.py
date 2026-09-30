from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("s.device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
    )
)
