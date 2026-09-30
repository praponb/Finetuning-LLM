from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.email"),
        F.dayofweek(F.col("c.signup_date")).alias("signup_date_dayofweek"),
    )
)
