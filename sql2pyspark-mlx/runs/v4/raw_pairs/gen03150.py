from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "key").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("level").orderBy(F.col("key_min_status_code_high").desc())
w5 = Window.partitionBy("key").orderBy("status_code")
w6 = Window.partitionBy("key").orderBy(F.col("status_code_high").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "level",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    unpacked
    .select("level", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h3")
        .select("level", "id")
    )
)

verified = (
    unpacked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.max("status_code").over(w).alias("key_max_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)

with_windows = (
    metrics
    .withColumn("level_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w2))
    .withColumn("key_max_status_code_share", F.round((F.col("key_max_status_code") / F.sum("key_max_status_code").over(w)), 4))
    .withColumn("key_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w))
    .withColumn("key_min_status_code_high", F.min("status_code_high").over(w))
)

metrics_app_logs = (
    with_windows
    .select(
        "*",
        F.count("key_max_status_code_share").over(w2).alias("level_count_key_max_status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
    )
)

metrics_app_logs_2 = (
    metrics_app_logs
    .withColumn("status_code_percent_rank", F.percent_rank().over(w3))
    .withColumn("key_min_status_code_high_cume_dist", F.cume_dist().over(w4))
    .withColumn("status_code_rank", F.rank().over(w5))
)

result = (
    metrics_app_logs_2
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w6))
    .withColumn("level_min_status_code_share_2", F.min("status_code_share_2").over(w2))
    .withColumn("level_sum_status_code_share_2", F.sum("status_code_share_2").over(w2))
)
