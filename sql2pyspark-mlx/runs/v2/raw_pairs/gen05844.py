from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2024-12-01") & (F.col("session_id") == 35))
    .groupBy("event_type", F.month("event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
    .filter(F.col("click_duration_sec") < 65)
    .orderBy("event_type")
)
