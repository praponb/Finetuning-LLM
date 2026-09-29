from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 291) & (F.col("session_id") == 15))
    .select(
        "duration_sec",
        "session_id",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
    .orderBy("event_time_day")
)
