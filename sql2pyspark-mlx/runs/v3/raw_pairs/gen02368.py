from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("c.segment"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.datediff(F.current_date(), F.col("s.started_at")).alias("days_since_started_at"),
    )
)
