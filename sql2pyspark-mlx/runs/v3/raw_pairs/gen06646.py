from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Chrome", "Safari"]))
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.upper(F.col("we.event_type")).alias("event_type_upper"),
    )
    .orderBy("session_id")
)
