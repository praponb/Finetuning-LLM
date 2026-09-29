from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("c.segment"),
    )
)
