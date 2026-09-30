from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("host_length").desc_nulls_last())
w2 = Window.partitionBy("k").orderBy(F.col("host_length").desc_nulls_first())
w3 = Window.partitionBy("host").orderBy(F.col("host_length").desc())
w4 = Window.orderBy(F.col("host_length").desc_nulls_first())
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("level")
w7 = Window.partitionBy("host", "level")
w8 = Window.partitionBy("host").orderBy(F.col("host_length_percent_rank").asc_nulls_first())
w9 = Window.partitionBy("host_length_rank_share_tier").orderBy(F.col("num_ks").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("k", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNotNull() & (F.col("service") != "billing"))
    .select(
        "id",
        "service",
        "host",
        "level",
        "k",
        "v",
        F.length("host").alias("host_length"),
        F.upper("service").alias("service_upper"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "host",
        "level",
        "k",
        "v",
        F.cume_dist().over(w).alias("host_length_cume_dist"),
        F.rank().over(w2).alias("host_length_rank"),
        F.rank().over(w3).alias("host_length_rank_2"),
        F.percent_rank().over(w4).alias("host_length_percent_rank"),
        F.stddev_samp("host_length").over(w5).alias("service_stddev_samp_host_length"),
    )
)

base = (
    with_windows
    .filter(F.col("host_length_cume_dist") <= 0.42)
    .withColumn("host_lower", F.lower("host"))
)

enriched = (
    base
    .filter(F.col("k") == "trace")
    .select(
        "host",
        "k",
        "v",
        "host_length_cume_dist",
        "host_length_rank",
        "host_length_rank_2",
        F.round((F.col("host_length_rank") / F.sum("host_length_rank").over(w6)), 4).alias("host_length_rank_share"),
        F.count("host_length_cume_dist").over(w7).alias("host_count_host_length_cume_dist"),
        F.avg("host_length_rank").over(w6).alias("level_avg_host_length_rank"),
        F.cume_dist().over(w8).alias("host_length_percent_rank_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("host_length_cume_dist") < 0.57)
    .select(
        "host",
        "k",
        "v",
        "host_length_cume_dist",
        "host_length_rank",
        "host_length_rank_2",
        "host_length_rank_share",
        "host_count_host_length_cume_dist",
        "level_avg_host_length_rank",
        "host_length_percent_rank_cume_dist",
        F.least("host_length_percent_rank_cume_dist", "host_length_cume_dist").alias("least_host_length_percent_rank_cume_dist_host_length_cume_dist"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("k").isNotNull() & (F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "k",
        "v",
        "host_length_cume_dist",
        "host_length_rank_2",
        "host_length_rank_share",
        "host_count_host_length_cume_dist",
        "host_length_percent_rank_cume_dist",
        "k_filled",
        F.coalesce("least_host_length_percent_rank_cume_dist_host_length_cume_dist", F.lit(0)).alias("least_host_length_percent_rank_cume_dist_host_length_cume_dist_filled"),
        F.round("host_length_rank", 2).alias("host_length_rank_rounded"),
    )
)

totals = (
    valid
    .groupBy(
        F.when(F.col("host_length_rank_share") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("host_length_rank_share_tier"),
        F.when(F.col("host_length_cume_dist") >= 0.65, "PREMIUM").otherwise("STANDARD").alias("host_length_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("host_length_rank_share", [0.25, 0.5, 0.75]).alias("host_length_rank_share_quartiles"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.countDistinct("k").alias("distinct_ks"),
        F.percentile_approx("host_length_rank_rounded", 0.5).alias("host_length_rank_rounded_p50"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .withColumn("num_ks_percent_rank", F.percent_rank().over(w9))
)

result = (
    totals
    .select(
        "host_length_rank_share_tier",
        "host_length_cume_dist_tier",
        "num_ks",
        "distinct_ks",
        "num_ks_percent_rank",
    )
)
