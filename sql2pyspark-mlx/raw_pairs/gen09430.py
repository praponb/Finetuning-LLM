from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy(F.year("log_ts")).orderBy(F.col("latency_ms_minus_status_code_max_2h").desc())
w5 = Window.partitionBy(F.date_trunc("week", "log_ts"), "level")
w6 = Window.partitionBy(F.month("log_ts"), "level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

kept_keys = (
    lines
    .select("service", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") >= 395)
        .select("service", "level")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = kept_rows.withColumn("status_code_max_2h", F.max("status_code").over(w))

filtered = (
    scored
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        "status_code_max_2h",
        (F.col("latency_ms") - F.col("status_code_max_2h")).alias("latency_ms_minus_status_code_max_2h"),
    )
)

result = (
    filtered
    .select(
        "id",
        "attr_key",
        "v",
        "latency_ms_minus_status_code_max_2h",
        F.round((F.col("status_code_max_2h") / F.sum("status_code_max_2h").over(w2)), 4).alias("status_code_max_2h_share"),
        F.corr("status_code", "latency_ms").over(w3).alias("corr_status_code_latency_ms"),
        F.percent_rank().over(w4).alias("latency_ms_minus_status_code_max_2h_percent_rank"),
        F.avg("latency_ms_minus_status_code_max_2h").over(w5).alias("log_ts_period_avg_latency_ms_minus_status_code_max_2h"),
        F.avg("latency_ms_minus_status_code_max_2h").over(w6).alias("log_ts_month_avg_latency_ms_minus_status_code_max_2h"),
    )
)
