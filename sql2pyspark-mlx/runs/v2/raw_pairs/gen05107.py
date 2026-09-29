from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Safari", "Firefox", "Edge"]))
    .select(
        F.col("s.customer_id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.session_id"),
    )
)
