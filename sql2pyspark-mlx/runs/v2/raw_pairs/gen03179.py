from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 304)
    .select(
        "duration_sec",
        "event_time",
        "event_type",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
    )
)
