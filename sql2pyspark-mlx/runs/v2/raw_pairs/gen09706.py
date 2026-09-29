from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.email"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("s.country"),
        F.col("s.started_at"),
    )
)
