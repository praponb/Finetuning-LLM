from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("k", "level")
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("latency_ms_rank").desc_nulls_last())

exploded = (
    df_app_logs
    .select("id", "host", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

filtered = (
    exploded
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "v",
        F.coalesce("k", F.lit("Unknown")).alias("k_filled"),
    )
)

remaining_keys = (
    filtered
    .select("level", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms") >= 1426)
        .select("level", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_rank", F.rank().over(w2))
)

filtered_app_logs = (
    scored
    .filter(F.col("level") == "WARN")
    .select(
        "*",
        F.least("latency_ms_share", "latency_ms").alias("least_latency_ms_share_latency_ms"),
        F.concat_ws("/", "level", "k").alias("level_k_key"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled_2"),
    )
)

metrics = (
    filtered_app_logs
    .select(
        "*",
        F.max("latency_ms_rank").over(w3).alias("k_max_latency_ms_rank"),
        F.sum("latency_ms_rank").over(w4).alias("level_sum_latency_ms_rank"),
        F.percent_rank().over(w5).alias("latency_ms_rank_percent_rank"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("latency_ms_share") >= 0.61, "LARGE").otherwise("SMALL").alias("latency_ms_share_tier"),
        F.when(F.col("least_latency_ms_share_latency_ms") >= 85, "PREMIUM").otherwise("STANDARD").alias("least_latency_ms_share_latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "WARN", F.col("latency_ms_rank")).otherwise(0)).alias("warn_latency_ms_rank"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.max(F.when(F.col("level") == "INFO", F.col("k_max_latency_ms_rank"))).alias("info_max_k_max_latency_ms_rank"),
    )
    .filter(F.col("latency_ms_bit_or") > 1)
)
