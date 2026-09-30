from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 476, "high").when(F.col("duration_sec") >= 187, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.month("event_time").alias("event_month"),
    )
    .agg(
        F.count("*").alias("num_web_events"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
    .filter(F.col("num_web_events") >= 2)
    .orderBy("pct_checkout")
)
