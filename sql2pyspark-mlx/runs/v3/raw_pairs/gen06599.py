from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([15, 22, 27, 39]))
    .select(
        "event_type",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
    )
)
