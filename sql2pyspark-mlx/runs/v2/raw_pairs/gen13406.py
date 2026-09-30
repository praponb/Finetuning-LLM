from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
    .orderBy("signup_date")
)
