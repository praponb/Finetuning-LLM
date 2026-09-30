from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy(F.to_date("log_ts"), "service").orderBy(F.col("status_code").desc())
w_running = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w5 = Window.orderBy(F.col("corr_latency_ms_status_code").asc_nulls_first())
w_part = Window.partitionBy("level").orderBy(F.col("status_code_dense_rank").asc_nulls_first())
w7 = Window.partitionBy("service")
w8 = Window.partitionBy("level", "service")
w9 = Window.partitionBy(F.year("log_ts"), "attr_key").orderBy(F.col("status_code_rank").desc_nulls_last())
w10 = Window.partitionBy("attr_key").orderBy(F.col("status_code_rank_percent_rank").desc_nulls_last())
w11 = Window.orderBy("status_code_dense_rank")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    lines
    .filter(F.col("level").isin(["WARN", "DEBUG"]))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("attr_key_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w3))
    .withColumn("status_code_rank", F.rank().over(w_running))
)

base = (
    scored
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .select(
        "*",
        (F.col("attr_key_max_latency_ms") * F.col("latency_ms")).alias("attr_key_max_latency_ms_times_latency_ms"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.cume_dist().over(w5).alias("corr_latency_ms_status_code_cume_dist"),
        F.rank().over(w_part).alias("status_code_dense_rank_rank"),
        F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w7)), 4).alias("status_code_rank_share"),
        F.round((F.col("attr_key_max_latency_ms") / F.sum("attr_key_max_latency_ms").over(w8)), 4).alias("attr_key_max_latency_ms_share"),
        F.percent_rank().over(w9).alias("status_code_rank_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("attr_key_max_latency_ms_times_latency_ms").between(24, 56) & F.col("attr_key").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "attr_key", "level").alias("attr_key_level_key"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .select(
        "service",
        "latency_ms",
        "attr_key",
        "attr_value",
        "attr_key_max_latency_ms",
        "status_code_dense_rank",
        "status_code_rank",
        "corr_latency_ms_status_code_cume_dist",
        "status_code_rank_share",
        "status_code_rank_percent_rank",
        "attr_key_level_key",
        "attr_key_clean",
        F.length("service").alias("service_length"),
    )
)

enriched_app_logs = (
    cleaned
    .select(
        "attr_value",
        "attr_key_max_latency_ms",
        "status_code_rank",
        "corr_latency_ms_status_code_cume_dist",
        "status_code_rank_share",
        "status_code_rank_percent_rank",
        "attr_key_level_key",
        "attr_key_clean",
        F.percent_rank().over(w10).alias("status_code_rank_percent_rank_percent_rank"),
        F.percent_rank().over(w11).alias("status_code_dense_rank_percent_rank"),
    )
)

report = (
    enriched_app_logs
    .select(
        "attr_value",
        "attr_key_max_latency_ms",
        "status_code_rank",
        "status_code_rank_percent_rank",
        "attr_key_clean",
        "status_code_rank_percent_rank_percent_rank",
        "status_code_dense_rank_percent_rank",
    )
)

result = (
    report
    .select("attr_key_max_latency_ms", "status_code_dense_rank_percent_rank")
    .orderBy(
        F.col("attr_key_max_latency_ms").desc(),
        "status_code_dense_rank_percent_rank",
    )
)
