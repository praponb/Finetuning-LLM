from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.signup_date") <= "2024-10-01") & (F.col("s.device") == "mobile"))
    .select(
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("c.city"),
    )
    .orderBy("city")
)
