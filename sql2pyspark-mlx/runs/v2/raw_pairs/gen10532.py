from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.session_id") == 22) & (F.col("we.event_type") == "purchase"))
    .select(
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("we.event_time"),
    )
)
