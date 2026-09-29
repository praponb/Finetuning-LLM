from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy("is_flagged")
w3 = Window.partitionBy("service").orderBy("is_flagged")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("service").isin(["checkout", "billing"]))
    .select(
        "id",
        "service",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    parsed
    .select(
        "id",
        "service",
        F.stddev_samp("is_flagged").over(w).alias("service_stddev_samp_is_flagged"),
        F.dense_rank().over(w2).alias("is_flagged_dense_rank"),
        F.rank().over(w3).alias("is_flagged_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("is_flagged_dense_rank") < 8)
    .withColumn("is_flagged_dense_rank_per_is_flagged_rank", F.round((F.col("is_flagged_dense_rank") / F.col("is_flagged_rank")), 2))
)

filtered = (
    valid
    .filter(F.col("is_flagged_rank") > 7)
    .select(
        "is_flagged_dense_rank",
        "is_flagged_rank",
        "is_flagged_dense_rank_per_is_flagged_rank",
        F.ceil("is_flagged_dense_rank").alias("is_flagged_dense_rank_whole"),
    )
)

base = (
    filtered
    .filter(F.col("is_flagged_dense_rank_whole") > 75)
    .withColumn("is_flagged_dense_rank_rounded", F.round("is_flagged_dense_rank", 2))
)

base_app_logs = (
    base
    .filter(F.col("is_flagged_rank") != 7)
    .select(
        "is_flagged_rank",
        "is_flagged_dense_rank_per_is_flagged_rank",
        "is_flagged_dense_rank_whole",
        "is_flagged_dense_rank_rounded",
        F.col("is_flagged_dense_rank_per_is_flagged_rank").cast("int").alias("is_flagged_dense_rank_per_is_flagged_rank_int"),
        F.col("is_flagged_dense_rank").cast("string").alias("is_flagged_dense_rank_str"),
    )
)

result = (
    base_app_logs
    .groupBy(
        F.when(F.col("is_flagged_dense_rank_per_is_flagged_rank") >= 79, "PREMIUM").otherwise("STANDARD").alias("is_flagged_dense_rank_per_is_flagged_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy(F.col("event_count").desc_nulls_first())
)
