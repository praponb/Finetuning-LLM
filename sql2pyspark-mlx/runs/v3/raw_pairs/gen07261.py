from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.started_at") < "2024-04-01") & F.col("c.city").isin(["Chiang Mai", "Berlin"]))
    .select(
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("c.name"),
    )
)
