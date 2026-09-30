from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.email").isNull())
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.name"),
    )
)
