from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([17, 18, 27, 28]) | (F.col("s.country") == "UK"))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.name"),
    )
    .orderBy("signup_date")
)
