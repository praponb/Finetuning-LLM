from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id").isin([7, 14, 21, 28]))
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.name"),
        F.col("c.country").alias("customer_country"),
    )
)
