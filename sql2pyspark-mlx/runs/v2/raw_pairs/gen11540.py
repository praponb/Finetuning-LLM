from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 154)
    .select("duration_sec", "event_type")
    .unionByName(
        df_web_events
        .filter(F.to_date("event_time") < "2024-03-19")
        .select("duration_sec", "event_type")
    )
    .distinct()
)
