from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-01-01")
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.name"),
        F.col("sbc.min_started_at"),
    )
)
