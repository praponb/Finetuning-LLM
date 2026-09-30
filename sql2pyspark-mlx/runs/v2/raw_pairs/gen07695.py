from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(9, 14) & (F.col("c.country") == "DE"))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
