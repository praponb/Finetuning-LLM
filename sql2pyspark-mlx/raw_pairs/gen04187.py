from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("attr_key")
w4 = Window.orderBy(F.col("status_code").desc())
w5 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("status_code_count_30m").desc())
w8 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.count("status_code").over(w).alias("status_code_count_30m"),
        F.max("latency_ms").over(w2).alias("latency_ms_max_15m"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
        F.cume_dist().over(w4).alias("status_code_cume_dist"),
        F.percent_rank().over(w5).alias("latency_ms_percent_rank"),
    )
)

scored_app_logs = (
    scored
    .filter(F.col("status_code_count_30m") <= 10)
    .select(
        "*",
        F.max("status_code_cume_dist").over(w2).alias("status_code_cume_dist_max_15m"),
        F.sum("latency_ms_max_15m").over(w6).alias("latency_ms_max_15m_sum_1h"),
        F.cume_dist().over(w7).alias("status_code_count_30m_cume_dist"),
        F.max("latency_ms").over(w8).alias("latency_ms_max_15m_2"),
    )
)

cleaned = (
    scored_app_logs
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_value",
        "status_code_count_30m",
        "latency_ms_max_15m",
        "corr_latency_ms_status_code",
        "status_code_cume_dist",
        "latency_ms_percent_rank",
        "status_code_cume_dist_max_15m",
        "latency_ms_max_15m_sum_1h",
        "status_code_count_30m_cume_dist",
        "latency_ms_max_15m_2",
        F.round("status_code_cume_dist", 0).alias("status_code_cume_dist_rounded"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

result = (
    cleaned
    .select(
        "service",
        F.col("status_code_cume_dist").alias("STATUS_CODE_CUME_DIST"),
        "status_code_cume_dist_rounded",
        F.col("corr_latency_ms_status_code").alias("CORR_LATENCY_MS_STATUS_CODE"),
        F.col("latency_ms_percent_rank").alias("LATENCY_MS_PERCENT_RANK"),
    )
    .unpivot(
        ids=["service"],
        values=["STATUS_CODE_CUME_DIST", "status_code_cume_dist_rounded", "CORR_LATENCY_MS_STATUS_CODE", "LATENCY_MS_PERCENT_RANK"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
