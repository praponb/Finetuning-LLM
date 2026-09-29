from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
)
