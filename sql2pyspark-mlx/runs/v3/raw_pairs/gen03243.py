from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.country") == "UK")
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.name"),
        F.col("c.email"),
    )
    .orderBy("customer_id")
)
