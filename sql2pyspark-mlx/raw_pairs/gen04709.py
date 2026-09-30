from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("trace_avg_status_code_high").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .select(
        "host",
        "level",
        "latency_ms",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "*",
        (F.col("has_fault") + F.col("status_code_high")).alias("has_fault_plus_status_code_high"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        (F.col("latency_ms") - F.col("has_fault_plus_status_code_high")).alias("latency_ms_minus_has_fault_plus_status_code_high"),
    )
)

kept_keys = (
    cleaned
    .select("level")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") <= 401)
        .select("level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

filtered_app_logs = (
    reconciled
    .filter(F.col("key").isNotNull())
    .select(
        "host",
        "level",
        "latency_ms",
        "key",
        "value",
        "status_code_high",
        "has_fault_plus_status_code_high",
        "latency_ms_minus_has_fault_plus_status_code_high",
        F.trim("key").alias("clean_key"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

base = (
    filtered_app_logs
    .filter(F.col("has_fault_plus_status_code_high") > 57)
    .withColumn("key_filled", F.coalesce("key", F.lit("Unknown")))
)

stats = (
    base
    .groupBy(
        "level",
        F.when(F.col("latency_ms") >= 1900, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.avg(F.when(F.col("key") == "trace", F.col("status_code_high"))).alias("trace_avg_status_code_high"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("trace_avg_status_code_high") >= 0)
    .withColumn("trace_avg_status_code_high_percent_rank", F.percent_rank().over(w))
)

result = (
    stats
    .select(
        "level",
        "latency_ms_tier",
        "num_keys",
        "cnt",
        "trace_avg_status_code_high_percent_rank",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "GROUP_B").otherwise("PRIORITY").alias("level_zone"),
    )
)
