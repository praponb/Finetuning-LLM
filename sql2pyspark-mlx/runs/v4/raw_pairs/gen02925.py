from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") <= 8900) & (F.col("c.signup_date") > "2025-09-20"))
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)
