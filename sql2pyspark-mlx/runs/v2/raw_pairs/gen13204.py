from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([22, 33, 37]) | F.col("session_id").isin([3, 7, 28]))
    .select(
        "id",
        "duration_sec",
        "event_type",
        "page",
        F.date_trunc("month", "event_time").alias("event_time_period"),
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
)
