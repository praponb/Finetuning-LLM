from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("c.segment"),
        F.col("c.email"),
        F.coalesce(F.col("s.country"), F.lit("N/A")).alias("country_filled"),
    )
    .orderBy("device", "id")
    .limit(50)
)
