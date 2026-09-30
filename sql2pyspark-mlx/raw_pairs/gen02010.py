from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("we.event_time"),
        F.lower(F.col("we.page")).alias("page_lower"),
    )
)
