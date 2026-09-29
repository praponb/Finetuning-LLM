from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .select("id", "session_id", "page")
    .intersect(
        df_web_events
        .filter(F.col("event_time") >= "2024-12-01")
        .select("id", "session_id", "page")
    )
    .orderBy("page")
)
