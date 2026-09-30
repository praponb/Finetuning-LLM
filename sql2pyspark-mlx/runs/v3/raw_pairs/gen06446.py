from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.country").alias("customer_country"),
    )
)
