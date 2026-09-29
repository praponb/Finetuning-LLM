from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        "duration_sec",
        "page",
        F.date_trunc("year", "event_time").alias("event_time_period"),
    )
)
