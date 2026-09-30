from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("level")
w_ordered = Window.partitionBy(F.date_trunc("month", "log_ts"), "key")
w4 = Window.partitionBy("level", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("host")
w6 = Window.partitionBy("level").orderBy(F.col("corr_latency_ms_status_code_max_1h").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_15m"),
        F.corr("latency_ms", "status_code").over(w_part).alias("corr_latency_ms_status_code"),
    )
)

base = (
    with_windows
    .filter(F.col("corr_latency_ms_status_code") > 0.0)
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "corr_latency_ms_status_code",
        F.col("status_code").cast("double").alias("status_code_num"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.sum("corr_latency_ms_status_code").over(w_ordered).alias("log_ts_period_sum_corr_latency_ms_status_code"),
        F.max("corr_latency_ms_status_code").over(w4).alias("corr_latency_ms_status_code_max_1h"),
        F.max("status_code_num").over(w_running).alias("host_max_status_code_num"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "log_ts",
        "key",
        "corr_latency_ms_status_code",
        "key_clean",
        "log_ts_period_sum_corr_latency_ms_status_code",
        "host_max_status_code_num",
        F.dense_rank().over(w6).alias("corr_latency_ms_status_code_max_1h_dense_rank"),
    )
)

result = (
    scored
    .select(
        "log_ts",
        "key",
        "corr_latency_ms_status_code",
        "key_clean",
        "log_ts_period_sum_corr_latency_ms_status_code",
        "host_max_status_code_num",
        "corr_latency_ms_status_code_max_1h_dense_rank",
        F.when(F.col("key").rlike("^(version|trace)$"), "EXTENDED").when(F.col("key").rlike("^(region)$"), "GROUP_B").otherwise("PRIORITY").alias("key_zone"),
    )
    .orderBy("log_ts_period_sum_corr_latency_ms_status_code", F.col("log_ts").desc())
)
