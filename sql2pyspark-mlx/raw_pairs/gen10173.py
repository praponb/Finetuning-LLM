from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w_part = Window.partitionBy("k", "level")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

matched_keys = (
    exploded
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms").between(1081, 1758))
        .select("id")
    )
)

verified = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select("*", F.dayofmonth("log_ts").alias("log_ts_day"))
)

scored = (
    base
    .select(
        "id",
        "latency_ms",
        "k",
        "log_ts_day",
        F.stddev_samp("latency_ms").over(w).alias("k_stddev_samp_latency_ms"),
        F.sum("latency_ms").over(w_part).alias("k_sum_latency_ms"),
        F.max("log_ts_day").over(w3).alias("log_ts_day_max_30m"),
    )
)

valid = (
    scored
    .filter(F.col("latency_ms") != 1878)
    .select(
        "id",
        "latency_ms",
        "k",
        "log_ts_day",
        "k_stddev_samp_latency_ms",
        "k_sum_latency_ms",
        "log_ts_day_max_30m",
        F.greatest("log_ts_day", "latency_ms").alias("greatest_log_ts_day_latency_ms"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("greatest_log_ts_day_latency_ms").between(32, 52))
    .select(
        "latency_ms",
        "k",
        "log_ts_day",
        "k_sum_latency_ms",
        "log_ts_day_max_30m",
        "greatest_log_ts_day_latency_ms",
        "resolved_k",
        F.round((F.col("greatest_log_ts_day_latency_ms") * 1.1), 2).alias("adjusted_greatest_log_ts_day_latency_ms"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k_2"),
    )
)

result = (
    valid_app_logs
    .rollup("k")
    .agg(
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.max(F.when(F.col("k") == "region", F.col("log_ts_day"))).alias("region_max_log_ts_day"),
        F.count("*").alias("cnt"),
    )
)
