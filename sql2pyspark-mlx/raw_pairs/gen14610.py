from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.month("log_ts_day"), "k")
w4 = Window.partitionBy("k").orderBy(F.col("latency_ms_share").desc(), "id", "k")
w5 = Window.partitionBy("k").orderBy("is_trace", "id", "k")
w6 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

base = (
    lines
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "service",
        "latency_ms",
        "k",
        "value",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.when(F.col("k") == "trace", 1).otherwise(0).alias("is_trace"),
    )
)

with_windows = (
    base
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("is_trace_avg_6", F.avg("is_trace").over(w2))
    .withColumn("log_ts_day_month_max_latency_ms", F.max("latency_ms").over(w3))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_app_logs = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w6))
    .filter(F.col("row_num_2") == 1)
)

result = (
    ranked_app_logs
    .groupBy(
        F.when(F.col("is_trace_avg_6") >= 78, "LARGE").otherwise("SMALL").alias("is_trace_avg_6_tier"),
        F.date_trunc("WEEK", "log_ts_day").alias("week_start"),
        F.date_trunc("QUARTER", "log_ts_day").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("k") == "trace", F.col("log_ts_day_month_max_latency_ms")).otherwise(0)).alias("trace_log_ts_day_month_max_latency_ms"),
        F.round(F.avg("is_trace"), 2).alias("avg_is_trace"),
        F.count_if(F.col("k") == "version").alias("version_events"),
    )
    .orderBy(F.col("version_events").desc())
)
