from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("s.country").alias("session_country"),
    )
)
