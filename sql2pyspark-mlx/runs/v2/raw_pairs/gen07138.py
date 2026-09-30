from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 31, 34, 40]))
    .select("id", "event_type", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2023-06-01")
        .select("id", "event_type", "duration_sec")
    )
    .distinct()
    .orderBy("duration_sec")
)
