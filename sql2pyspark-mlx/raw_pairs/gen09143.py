from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy(F.year("log_ts"), "attr_key")
w3 = Window.partitionBy(F.year("log_ts"))
w4 = Window.partitionBy("attr_key", "service")
w5 = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("service", "attr_key")
w7 = Window.partitionBy("service", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("service")
w_part = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w11 = Window.partitionBy("level")
w12 = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "service").orderBy("corr_status_code_latency_ms_avg_15m")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.sum("status_code").over(w).alias("status_code_sum_1h"),
        F.avg("status_code").over(w2).alias("log_ts_year_avg_status_code"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
        F.corr("status_code", "latency_ms").over(w4).alias("corr_status_code_latency_ms"),
        F.avg("latency_ms").over(w5).alias("latency_ms_avg_15m"),
    )
)

base = (
    metrics
    .filter((F.col("log_ts") >= F.lit("2025-06-01 12:00:00").cast("timestamp")) & (F.col("corr_status_code_latency_ms") >= -0.3))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        "status_code_sum_1h",
        "log_ts_year_avg_status_code",
        "corr_status_code_latency_ms",
        "latency_ms_avg_15m",
        F.when(F.col("service").isin(["checkout", "search"]), "Priority").otherwise("Other").alias("service_group"),
        F.col("log_ts").cast("date").alias("log_ts_day"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "attr_value",
        "status_code_sum_1h",
        "log_ts_year_avg_status_code",
        "service_group",
        F.count("log_ts_year_avg_status_code").over(w6).alias("service_count_log_ts_year_avg_status_code"),
        F.avg("corr_status_code_latency_ms").over(w7).alias("corr_status_code_latency_ms_avg_15m"),
        F.count("latency_ms_avg_15m").over(w8).alias("latency_ms_avg_15m_count_1h"),
    )
)

valid = (
    with_windows
    .filter(F.col("corr_status_code_latency_ms_avg_15m") != 0)
    .withColumn("clean_service", F.trim("service"))
    .withColumn("latency_ms_per_service_count_log_ts_year_avg_status_code", F.round((F.col("latency_ms") / F.col("service_count_log_ts_year_avg_status_code")), 2))
)

filtered = (
    valid
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "attr_value",
        "log_ts_year_avg_status_code",
        "service_group",
        "service_count_log_ts_year_avg_status_code",
        "corr_status_code_latency_ms_avg_15m",
        "latency_ms_avg_15m_count_1h",
        "latency_ms_per_service_count_log_ts_year_avg_status_code",
        F.floor("log_ts_year_avg_status_code").alias("log_ts_year_avg_status_code_whole"),
    )
)

with_windows_app_logs = (
    filtered
    .filter(F.col("log_ts_year_avg_status_code") == 269)
    .select(
        "*",
        F.sum("log_ts_year_avg_status_code").over(w_grp).alias("service_sum_log_ts_year_avg_status_code"),
        F.count("log_ts_year_avg_status_code_whole").over(w_part).alias("log_ts_year_avg_status_code_whole_count_2h"),
        F.round((F.col("service_count_log_ts_year_avg_status_code") / F.sum("service_count_log_ts_year_avg_status_code").over(w11)), 4).alias("service_count_log_ts_year_avg_status_code_share"),
        F.cume_dist().over(w12).alias("corr_status_code_latency_ms_avg_15m_cume_dist"),
        F.min("service_count_log_ts_year_avg_status_code").over(w_grp).alias("service_min_service_count_log_ts_year_avg_status_code"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "level",
        "service_count_log_ts_year_avg_status_code_share",
        F.col("latency_ms_per_service_count_log_ts_year_avg_status_code").alias("LATENCY_MS_PER_SERVICE_COUNT_LOG_TS_YEAR_AVG_STATUS_CODE_VAL"),
        F.col("corr_status_code_latency_ms_avg_15m_cume_dist").alias("CORR_STATUS_CODE_LATENCY_MS_AVG_15M_CUME_DIST_VAL"),
    )
    .unpivot(
        ids=["level"],
        values=["service_count_log_ts_year_avg_status_code_share", "LATENCY_MS_PER_SERVICE_COUNT_LOG_TS_YEAR_AVG_STATUS_CODE_VAL", "CORR_STATUS_CODE_LATENCY_MS_AVG_15M_CUME_DIST_VAL"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
