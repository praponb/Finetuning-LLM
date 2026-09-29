from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([6, 15, 16, 24]))
    .select("id", "page", "event_type")
    .intersect(
        df_web_events
        .filter(F.col("event_time") >= "2024-08-05")
        .select("id", "page", "event_type")
    )
)
