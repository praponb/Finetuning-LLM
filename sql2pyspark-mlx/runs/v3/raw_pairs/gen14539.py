from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        "session_id",
        F.hour("event_time").alias("event_time_hour"),
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
    .orderBy(F.col("event_time_hour").desc())
)
