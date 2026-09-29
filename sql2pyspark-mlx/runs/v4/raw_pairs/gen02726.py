from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w_running = Window.orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("level").orderBy(F.col("is_flagged_whole").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("latency_ms") >= 2374)
    .select(
        "log_ts",
        "level",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    parsed
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "level",
        "latency_ms",
        "is_flagged",
        "status_code_high",
        (F.col("latency_ms") + F.col("status_code_high")).alias("latency_ms_plus_status_code_high"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("latency_ms").between(1206, 2293))
    .withColumn("is_flagged_whole", F.floor("is_flagged"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.max("status_code_high").over(w).alias("level_max_status_code_high"),
        F.cume_dist().over(w_running).alias("status_code_high_cume_dist"),
    )
)

with_windows = (
    metrics
    .withColumn("is_flagged_whole_percent_rank", F.percent_rank().over(w3))
    .withColumn("level_max_latency_ms_plus_status_code_high", F.max("latency_ms_plus_status_code_high").over(w))
)

result = (
    with_windows
    .select(
        "level",
        "is_flagged",
        "status_code_high",
        "latency_ms_plus_status_code_high",
        "k_clean",
        "is_flagged_whole",
        "level_max_status_code_high",
        "is_flagged_whole_percent_rank",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("level_max_status_code_high").desc())
)
