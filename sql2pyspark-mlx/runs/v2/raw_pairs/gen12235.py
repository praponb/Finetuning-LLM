from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.country") == "UK")
    .select(
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.length(F.col("s.device")).alias("device_length"),
    )
)
