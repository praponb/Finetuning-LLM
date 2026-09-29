from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2023-10-01") & (F.col("duration_sec") >= 238))
    .select(
        "event_type",
        F.lower("page").alias("page_lower"),
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
