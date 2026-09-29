from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
)
