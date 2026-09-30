from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(~F.col("s.country").isin(["SG", "UK"]) & ~F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.city"),
    )
)
