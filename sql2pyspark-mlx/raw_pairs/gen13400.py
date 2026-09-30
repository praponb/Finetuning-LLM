from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service", "key")
w3 = Window.partitionBy("service").orderBy("service_avg_status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("running_max_service_avg_status_code").desc_nulls_first())
w5 = Window.partitionBy("service_avg_status_code_tier").orderBy(F.col("warn_status_code").desc())

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
        F.explode("context").alias("key", "value"),
    )
)

scored = (
    lines
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "key",
        "value",
        F.first("key", ignorenulls=True).over(w).alias("first_known_key"),
        F.avg("status_code").over(w2).alias("service_avg_status_code"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "service_avg_status_code",
        F.count("service_avg_status_code").over(w3).alias("running_count_service_avg_status_code"),
        F.sum("service_avg_status_code").over(w3).alias("running_sum_service_avg_status_code"),
        F.max("service_avg_status_code").over(w3).alias("running_max_service_avg_status_code"),
        F.avg("service_avg_status_code").over(w3).alias("running_avg_service_avg_status_code"),
        F.first("service_avg_status_code", ignorenulls=True).over(w3).alias("first_known_service_avg_status_code"),
    )
)

remaining_keys = (
    metrics
    .select("id", "level")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") != 2281)
        .select("id", "level")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "level",
        "status_code",
        "service_avg_status_code",
        "running_count_service_avg_status_code",
        "running_max_service_avg_status_code",
        "first_known_service_avg_status_code",
        F.cume_dist().over(w4).alias("running_max_service_avg_status_code_cume_dist"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("service_avg_status_code") >= 438, "HIGH").otherwise("LOW").alias("service_avg_status_code_tier"),
        "level",
    )
    .agg(
        F.sum(F.when(F.col("level") == "WARN", F.col("status_code")).otherwise(0)).alias("warn_status_code"),
        F.sum("running_max_service_avg_status_code_cume_dist").alias("total_running_max_service_avg_status_code_cume_dist"),
    )
    .withColumn("warn_status_code_dense_rank", F.dense_rank().over(w5))
)

result = (
    totals
    .select(
        "service_avg_status_code_tier",
        "level",
        "warn_status_code",
        "total_running_max_service_avg_status_code_cume_dist",
    )
    .orderBy(F.col("level").desc())
)
