from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 24, 37]))
    .select(
        "page",
        "event_time",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
        F.length("event_type").alias("event_type_length"),
    )
    .orderBy("page", F.col("event_type_length").desc())
)
