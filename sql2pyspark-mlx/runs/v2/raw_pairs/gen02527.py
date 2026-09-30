from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([22, 24]))
    .select("duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2024-10-01")
        .select("duration_sec")
    )
    .orderBy("duration_sec")
)
