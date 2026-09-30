from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"), "session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
    .orderBy("avg_duration_sec", "total_duration_sec")
)
