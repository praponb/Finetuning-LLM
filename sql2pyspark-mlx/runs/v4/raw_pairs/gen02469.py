from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high").asc_nulls_first())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host", "level")
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level", "host")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "host",
        "level",
        "latency_ms",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    unpacked
    .select("level")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["DEBUG", "ERROR", "WARN"]))
        .select("level")
    )
)

reconciled = (
    unpacked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.percent_rank().over(w).alias("status_code_high_percent_rank"),
        F.max("latency_ms").over(w2).alias("host_max_latency_ms"),
        F.count("has_fault").over(w3).alias("host_count_has_fault"),
        F.min("latency_ms").over(w4).alias("level_min_latency_ms"),
        F.avg("status_code_high").over(w5).alias("level_avg_status_code_high"),
    )
)

result = (
    metrics
    .select(
        "host",
        "level",
        "latency_ms",
        "value",
        "has_fault",
        "status_code_high",
        "status_code_high_percent_rank",
        "host_max_latency_ms",
        "host_count_has_fault",
        F.when(F.col("host").rlike("^(h1|h2)$"), "GROUP_B").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("host_count_has_fault").desc())
)
