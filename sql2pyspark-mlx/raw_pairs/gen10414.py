from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k")
w2 = Window.partitionBy("k", "service")
w3 = Window.partitionBy("service").orderBy(F.col("k_stddev_samp_flag_bit1").desc_nulls_first())
w4 = Window.partitionBy("service").orderBy("k_min_status_code_high")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "service",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    unpacked
    .filter(F.col("status_code_high") <= 2)
    .select(
        "service",
        "flag_bit1",
        F.stddev_samp("flag_bit1").over(w_running).alias("k_stddev_samp_flag_bit1"),
        F.min("status_code_high").over(w2).alias("k_min_status_code_high"),
        F.max("latency_ms").over(w_running).alias("k_max_latency_ms"),
    )
)

base = (
    scored
    .filter(~F.col("service").isin(["checkout", "auth", "search"]))
    .select(
        "*",
        (F.col("k_min_status_code_high") + F.col("k_max_latency_ms")).alias("k_min_status_code_high_plus_k_max_latency_ms"),
    )
)

enriched = (
    base
    .select(
        "k_stddev_samp_flag_bit1",
        "k_min_status_code_high",
        "k_min_status_code_high_plus_k_max_latency_ms",
        F.rank().over(w3).alias("k_stddev_samp_flag_bit1_rank"),
        F.rank().over(w4).alias("k_min_status_code_high_rank"),
    )
)

result = (
    enriched
    .withColumn("k_min_status_code_high_plus_k_max_latency_ms_tier", F.when(F.col("k_min_status_code_high_plus_k_max_latency_ms") >= 81, "HIGH").otherwise("LOW"))
    .withColumn("k_min_status_code_high_rank_tier", F.when(F.col("k_min_status_code_high_rank") >= 9, "HIGH").otherwise("LOW"))
    .cube(
        "k_min_status_code_high_plus_k_max_latency_ms_tier",
        "k_min_status_code_high_rank_tier",
    )
    .agg(
        F.percentile_approx("k_min_status_code_high", 0.95).alias("k_min_status_code_high_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("k_min_status_code_high", 0.95).alias("k_min_status_code_high_p95_2"),
        F.grouping("k_min_status_code_high_plus_k_max_latency_ms_tier").alias("gid"),
    )
    .orderBy("row_count")
)
