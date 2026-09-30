from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 479, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec"))).alias("add_to_cart_total_duration_sec"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
