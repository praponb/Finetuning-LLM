from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(~F.col("s.browser").isin(["Safari", "Chrome", "Firefox"]))
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("we.page"),
    )
    .orderBy("event_time", "page")
)
