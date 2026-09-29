from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        F.avg("status_code").over(w).alias("running_avg_status_code"),
        F.min("status_code").over(w).alias("running_min_status_code"),
        F.sum("status_code").over(w).alias("running_sum_status_code"),
        F.max("status_code").over(w).alias("running_max_status_code"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    latest
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("level").isin(["INFO", "DEBUG", "WARN"]))
        .select("id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
        F.percentile_approx("running_sum_status_code", 0.95).alias("running_sum_status_code_p95"),
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
        F.sum(F.when(F.col("key") == "version", F.col("running_max_status_code")).otherwise(0)).alias("version_running_max_status_code"),
    )
    .filter(F.col("region_rows") >= 1)
)
