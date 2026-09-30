from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-01-04")
    .select("event_type", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2023-10-01")
        .select("event_type", "session_id")
    )
)
