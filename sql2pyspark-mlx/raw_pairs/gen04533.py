from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.session_id").isin([19, 21, 22]))
    .select(
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.concat(F.col("we.page"), F.lit(", "), F.col("we.event_type")).alias("page_event_type"),
    )
)
