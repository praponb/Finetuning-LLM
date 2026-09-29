from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 29, 31, 40]))
    .select("page", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.to_date("event_time") < "2024-06-01")
        .select("page", "duration_sec")
    )
    .orderBy("duration_sec")
)
