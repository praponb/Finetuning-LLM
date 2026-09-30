from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host").orderBy("status_code")
w2 = Window.partitionBy("host").orderBy("status_code")
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("key", "host")

exploded = (
    df_app_logs
    .select("id", "host", "status_code", F.explode_outer("context").alias("key", "v"))
)

valid = (
    exploded
    .filter(F.col("host") == "h4")
    .withColumn("status_code_scaled", F.round((F.col("status_code") * 1.1), 2))
    .withColumn("key_title", F.initcap(F.trim("key")))
)

cleaned = (
    valid
    .filter(F.col("key").isNotNull() & ~F.col("key").isin(["version", "trace"]))
    .withColumn("status_code_times_status_code_scaled", (F.col("status_code") * F.col("status_code_scaled")))
)

with_windows = (
    cleaned
    .select(
        "key",
        "v",
        "key_title",
        "status_code_times_status_code_scaled",
        F.rank().over(w).alias("status_code_rank"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.stddev_samp("status_code_times_status_code_scaled").over(w3).alias("key_stddev_samp_status_code_times_status_code_scaled"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
    )
)

cleaned_app_logs = (
    with_windows
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        (F.col("status_code_cume_dist") * F.col("key_stddev_samp_status_code_times_status_code_scaled")).alias("status_code_cume_dist_times_key_stddev_samp_status_code_times_status_code_scaled"),
        F.least("status_code_rank", "status_code_times_status_code_scaled").alias("least_status_code_rank_status_code_times_status_code_scaled"),
    )
)

result = (
    cleaned_app_logs
    .withColumn("status_code_cume_dist_tier", F.when(F.col("status_code_cume_dist") >= 0.67, "HIGH").otherwise("LOW"))
    .withColumn("status_code_share_2_tier", F.when(F.col("status_code_share_2") >= 0.73, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status_code_cume_dist_tier", "status_code_share_2_tier"], ["status_code_cume_dist_tier"], []],
        "status_code_cume_dist_tier",
        "status_code_share_2_tier",
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.countDistinct("key").alias("distinct_keys_2"),
        F.round(F.avg("key_stddev_samp_status_code_times_status_code_scaled"), 2).alias("avg_key_stddev_samp_status_code_times_status_code_scaled"),
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
    )
    .filter(F.col("avg_key_stddev_samp_status_code_times_status_code_scaled") > 0)
    .orderBy("trace_count")
)
