from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("level", "key").orderBy("status_code")
w5 = Window.partitionBy("key").orderBy("status_code_share_2")
w6 = Window.partitionBy("level").orderBy(F.col("status_code_share_2").desc_nulls_last())
w7 = Window.partitionBy("key").orderBy(F.col("status_code_share_2").desc_nulls_last())
w_ordered = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
        F.rank().over(w2).alias("status_code_rank"),
    )
)

with_windows_app_logs = (
    with_windows
    .withColumn("key_stddev_samp_status_code_share", F.stddev_samp("status_code_share").over(w3))
    .withColumn("key_min_status_code_share", F.min("status_code_share").over(w3))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w4))
    .withColumn("status_code_share_2_cume_dist", F.cume_dist().over(w5))
    .withColumn("status_code_share_2_rank", F.rank().over(w6))
)

scored = (
    with_windows_app_logs
    .filter(F.col("status_code_share") <= 0.45)
    .select(
        "id",
        "service",
        "level",
        "key",
        "status_code_share_2",
        "status_code_rank",
        "status_code_share_2_cume_dist",
        "status_code_share_2_rank",
        F.rank().over(w7).alias("status_code_share_2_rank_2"),
        F.count("status_code_share").over(w_ordered).alias("level_count_status_code_share"),
        F.max("status_code_share_2_cume_dist").over(w_ordered).alias("level_max_status_code_share_2_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("key").isNotNull())
    .withColumn("least_status_code_share_2_rank_status_code_rank", F.least("status_code_share_2_rank", "status_code_rank"))
)

daily = (
    filtered
    .groupBy(
        F.when(F.col("least_status_code_share_2_rank_status_code_rank") >= 74, "LARGE").otherwise("SMALL").alias("least_status_code_share_2_rank_status_code_rank_tier"),
        "service",
        F.when(F.col("status_code_share_2_cume_dist") >= 0.6, "LARGE").otherwise("SMALL").alias("status_code_share_2_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("status_code_share_2_rank") / F.nullif(F.count(F.when(F.col("key") == "version", 1)), F.lit(0)), 2).alias("status_code_share_2_rank_per_version"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.max("status_code_share_2").alias("max_status_code_share_2"),
    )
    .filter(F.col("status_code_share_2_rank_per_version") > 0)
)

result = (
    daily
    .select(
        "least_status_code_share_2_rank_status_code_rank_tier",
        "service",
        "status_code_share_2_cume_dist_tier",
        "status_code_share_2_rank_per_version",
        "region_count",
        "max_status_code_share_2",
    )
    .orderBy(F.col("max_status_code_share_2").desc())
)
