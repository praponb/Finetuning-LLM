from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 506, "high").when(F.col("duration_sec") >= 202, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .filter(F.col("purchase_count") < 3)
    .orderBy("duration_sec_bucket", "event_type")
)
