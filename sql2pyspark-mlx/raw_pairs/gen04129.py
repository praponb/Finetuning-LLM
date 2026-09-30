from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.email"),
        F.coalesce(F.col("c.segment"), F.lit("unassigned")).alias("segment_filled"),
    )
    .orderBy("customer_id", F.col("id").desc())
)
