from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/home")
    .select("id", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_time") >= "2025-10-18")
        .select("id", "session_id")
    )
)
