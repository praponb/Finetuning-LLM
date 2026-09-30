from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-02-01") & F.col("session_id").isin([9, 18, 27, 34]))
    .select(
        "page",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
    .orderBy(F.col("event_time_day").desc())
)
