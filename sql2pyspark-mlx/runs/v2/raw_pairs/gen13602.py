from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") < 9800) & (F.col("s.customer_id") == 6))
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
)
