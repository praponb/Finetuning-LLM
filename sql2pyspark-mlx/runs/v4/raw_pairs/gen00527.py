from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_type",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
