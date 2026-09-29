from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") > 5600) & F.col("c.name").isNotNull())
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)
