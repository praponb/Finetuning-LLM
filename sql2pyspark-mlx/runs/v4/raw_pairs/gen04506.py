from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 135)
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "search")
        .select("id", "duration_sec")
    )
)
