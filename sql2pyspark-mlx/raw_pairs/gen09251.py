from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max(F.when(F.col("event_type") == "purchase", F.col("duration_sec"))).alias("purchase_max_duration_sec"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
    )
    .filter(F.col("purchase_max_duration_sec") > 0)
)
