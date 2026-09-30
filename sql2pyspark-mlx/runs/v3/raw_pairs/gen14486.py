from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-07-01")
    .select("id", "page", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([6, 18]))
        .select("id", "page", "duration_sec")
    )
    .distinct()
)
