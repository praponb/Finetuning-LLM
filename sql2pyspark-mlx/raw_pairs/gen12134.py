from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k")
w3 = Window.partitionBy(F.year("log_ts"), "k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("k").orderBy(F.col("k_min_latency_ms").desc_nulls_first())
w6 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

scored = (
    lines
    .filter(F.col("status_code").between(381, 440))
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        "v",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.min("latency_ms").over(w2).alias("k_min_latency_ms"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "log_ts",
        "k",
        "last_known_k",
        F.last("k_min_latency_ms", ignorenulls=True).over(w3).alias("last_known_k_min_latency_ms"),
        F.first("k_min_latency_ms", ignorenulls=True).over(w4).alias("first_known_k_min_latency_ms"),
        F.percent_rank().over(w_running).alias("k_min_latency_ms_percent_rank"),
        F.sum("k_min_latency_ms").over(w).alias("cumulative_k_min_latency_ms"),
        F.count("status_code").over(w6).alias("status_code_count_2h"),
    )
)

base = (
    with_windows
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (~F.col("k").eqNullSafe(F.lit("region"))))
    .select(
        "*",
        F.round("status_code_count_2h", 1).alias("status_code_count_2h_rounded"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows_2"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.percentile_approx("cumulative_k_min_latency_ms", [0.25, 0.5, 0.75]).alias("cumulative_k_min_latency_ms_quartiles"),
        F.countDistinct("k").alias("distinct_ks"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)
