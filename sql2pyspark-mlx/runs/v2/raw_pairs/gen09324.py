from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.is_active"),
        F.datediff(F.current_date(), F.col("s.started_at")).alias("days_since_started_at"),
    )
)
