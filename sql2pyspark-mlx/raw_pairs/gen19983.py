from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"), "event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("duration_sec_stddev") > 0)
    .orderBy(F.col("purchase_duration_sec").desc())
)
