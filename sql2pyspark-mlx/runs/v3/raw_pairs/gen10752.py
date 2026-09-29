from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([13, 24, 25]) & (F.col("duration_sec").isNotNull() | (F.col("duration_sec") > 215)))
    .select(
        "id",
        "event_type",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
