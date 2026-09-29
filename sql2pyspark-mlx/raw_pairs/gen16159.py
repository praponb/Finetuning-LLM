from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts"), "host")
w3 = Window.partitionBy("service").orderBy(F.col("running_avg_status_code").asc_nulls_first(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("log_ts_month_count_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

with_windows = (
    lines
    .filter(F.col("latency_ms") == 2247)
    .withColumn("running_count_status_code", F.count("status_code").over(w))
    .withColumn("running_avg_status_code", F.avg("status_code").over(w))
    .withColumn("running_min_status_code", F.min("status_code").over(w))
    .withColumn("running_sum_status_code", F.sum("status_code").over(w))
    .withColumn("log_ts_month_count_latency_ms", F.count("latency_ms").over(w2))
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "running_count_status_code",
        "running_avg_status_code",
        "running_min_status_code",
        "running_sum_status_code",
        "rn",
        F.max("status_code").over(w4).alias("running_max_status_code"),
        F.avg("status_code").over(w4).alias("running_avg_status_code_2"),
        F.min("status_code").over(w4).alias("running_min_status_code_2"),
        F.count("running_sum_status_code").over(w5).alias("running_sum_status_code_count_2h"),
    )
)

result = (
    scored
    .groupBy("service")
    .agg(
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.percentile_approx("running_count_status_code", [0.25, 0.5, 0.75]).alias("running_count_status_code_quartiles"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
