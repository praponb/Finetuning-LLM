from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "service",
        "log_ts",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "service",
        "log_ts",
        "status_code",
        "key",
        "status_code_high",
        (F.col("status_code") / F.nullif("has_fault", F.lit(0))).alias("status_code_per_has_fault"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

with_windows = (
    cleaned
    .select(
        "log_ts",
        "status_code",
        F.count("status_code_high").over(w).alias("status_code_high_count_2h"),
        F.stddev_samp("status_code").over(w2).alias("service_stddev_samp_status_code"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("service_stddev_samp_status_code") >= 414, "LARGE").otherwise("SMALL").alias("service_stddev_samp_status_code_tier"),
        F.when(F.col("status_code_high_count_2h") >= 13, "PREMIUM").otherwise("STANDARD").alias("status_code_high_count_2h_tier"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count("*").alias("cnt"),
        F.sum("status_code_high_count_2h").alias("total_status_code_high_count_2h"),
        F.percentile_approx("service_stddev_samp_status_code", [0.25, 0.5, 0.75]).alias("service_stddev_samp_status_code_quartiles"),
        F.percentile_approx("status_code_high_count_2h", [0.25, 0.5, 0.75]).alias("status_code_high_count_2h_quartiles"),
        F.percentile_approx("service_stddev_samp_status_code", 0.9).alias("service_stddev_samp_status_code_p90"),
    )
    .filter(F.col("total_status_code_high_count_2h") > 0)
)

result = (
    summary
    .select(
        "service_stddev_samp_status_code_tier",
        "status_code_high_count_2h_tier",
        "status_code_bit_or",
        "cnt",
        "total_status_code_high_count_2h",
        "service_stddev_samp_status_code_quartiles",
        "status_code_high_count_2h_quartiles",
        "service_stddev_samp_status_code_p90",
    )
    .orderBy("cnt")
)
