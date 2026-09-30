from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)
