from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.device") == "desktop")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.round(F.col("we.duration_sec"), 1).alias("duration_sec_rounded"),
    )
)
