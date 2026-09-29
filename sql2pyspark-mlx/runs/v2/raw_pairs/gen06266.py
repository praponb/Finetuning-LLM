from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_time",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.upper("event_type").alias("event_type_upper"),
    )
)
