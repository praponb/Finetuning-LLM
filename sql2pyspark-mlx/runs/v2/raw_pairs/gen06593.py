from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(~F.col("c.is_active") | (F.col("c.segment") != "Consumer"))
    .select(
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)
