from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-09-01")
    .select("id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") != 356)
        .select("id", "event_type")
    )
    .orderBy("event_type")
)
