from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 326) & F.hour("event_time").between(6, 10))
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)
