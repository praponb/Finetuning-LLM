from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_h3").desc_nulls_first())
w2 = Window.partitionBy(F.month("log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("host")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy(F.to_date("log_ts"), "host").orderBy(F.col("host_sum_is_h3").asc_nulls_first())
w6 = Window.orderBy(F.col("is_h3_cume_dist").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select(
        "id",
        "host",
        "log_ts",
        "value",
        F.when(F.col("host") == "h3", 1).otherwise(0).alias("is_h3"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "host",
        "log_ts",
        "value",
        "is_h3",
        F.cume_dist().over(w).alias("is_h3_cume_dist"),
        F.avg("is_h3").over(w2).alias("is_h3_avg_1h"),
        F.sum("is_h3").over(w3).alias("host_sum_is_h3"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "is_h3",
        "is_h3_avg_1h",
        F.avg("is_h3_cume_dist").over(w4).alias("is_h3_cume_dist_avg_30m"),
        F.percent_rank().over(w5).alias("host_sum_is_h3_percent_rank"),
        F.rank().over(w6).alias("is_h3_cume_dist_rank"),
        F.stddev_samp("is_h3_avg_1h").over(w3).alias("host_stddev_samp_is_h3_avg_1h"),
        F.sum("is_h3_avg_1h").over(w3).alias("host_sum_is_h3_avg_1h"),
    )
)

stats = (
    enriched
    .withColumn("is_h3_cume_dist_rank_tier", F.when(F.col("is_h3_cume_dist_rank") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("is_h3_tier", F.when(F.col("is_h3") >= 76, "HIGH").otherwise("LOW"))
    .cube("is_h3_cume_dist_rank_tier", "is_h3_tier")
    .agg(
        F.percentile_approx("host_stddev_samp_is_h3_avg_1h", [0.25, 0.5, 0.75]).alias("host_stddev_samp_is_h3_avg_1h_quartiles"),
        F.percentile_approx("host_stddev_samp_is_h3_avg_1h", 0.5).alias("host_stddev_samp_is_h3_avg_1h_p50"),
        F.sum("host_sum_is_h3_percent_rank").alias("total_host_sum_is_h3_percent_rank"),
        F.grouping("is_h3_cume_dist_rank_tier").alias("grouping_level"),
    )
    .filter(F.col("total_host_sum_is_h3_percent_rank") > 0)
)

result = (
    stats
    .select(
        "is_h3_cume_dist_rank_tier",
        "host_stddev_samp_is_h3_avg_1h_quartiles",
        "host_stddev_samp_is_h3_avg_1h_p50",
        "total_host_sum_is_h3_percent_rank",
        "grouping_level",
    )
    .orderBy("is_h3_cume_dist_rank_tier")
)
