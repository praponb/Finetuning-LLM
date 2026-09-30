from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy(F.to_date("log_ts"), "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("status_code") >= 348)
    .select(
        "*",
        F.count("status_code_high").over(w).alias("status_code_high_count_30m"),
    )
)

with_windows = (
    metrics
    .select(
        "has_fault",
        "status_code_high",
        F.avg("status_code_high_count_30m").over(w2).alias("log_ts_to_date_avg_status_code_high_count_30m"),
    )
)

valid = (
    with_windows
    .filter(F.col("status_code_high") == 1)
    .withColumn("status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m", (F.col("status_code_high") * F.col("log_ts_to_date_avg_status_code_high_count_30m")))
    .withColumn("log_ts_to_date_avg_status_code_high_count_30m_scaled", F.round((F.col("log_ts_to_date_avg_status_code_high_count_30m") * 0.85), 2))
)

result = (
    valid
    .groupBy(
        F.when(F.col("status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m") >= 79, "HIGH").otherwise("LOW").alias("status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m_tier"),
    )
    .agg(
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.round(F.avg("log_ts_to_date_avg_status_code_high_count_30m"), 2).alias("avg_log_ts_to_date_avg_status_code_high_count_30m"),
        F.max("status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m").alias("max_status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m"),
    )
)
