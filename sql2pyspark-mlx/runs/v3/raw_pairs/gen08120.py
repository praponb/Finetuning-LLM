from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("s.started_at") < "2025-03-15") & ~F.col("s.device").isin(["mobile", "tablet", "desktop"]))
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.name"),
        F.col("c.country").alias("customer_country"),
        F.col("c.is_active"),
    )
)
