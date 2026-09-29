from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.count("*").alias("web_event_count"),
    )
    .filter(F.col("min_duration_sec") > 250)
)
