from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
    .filter(F.col("click_duration_sec") < 34)
    .orderBy("page")
)
