from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("attr_key", "level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w7 = Window.partitionBy(F.year("log_ts"), "service").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w8 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w9 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("session_id_tier").orderBy(F.col("trace_status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        F.stddev_samp("status_code").over(w).alias("service_stddev_samp_status_code"),
        F.sum("status_code").over(w2).alias("cumulative_status_code"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
    )
)

with_windows = (
    metrics
    .filter(F.col("service") == "auth")
    .select(
        "*",
        F.sum("cumulative_status_code").over(w4).alias("cumulative_cumulative_status_code"),
        F.round((F.col("service_stddev_samp_status_code") / F.sum("service_stddev_samp_status_code").over(w5)), 4).alias("service_stddev_samp_status_code_share"),
        F.dense_rank().over(w6).alias("status_code_dense_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.round((F.col("service_stddev_samp_status_code") / F.sum("service_stddev_samp_status_code").over(w)), 4).alias("service_stddev_samp_status_code_share_2"),
    )
)

base = (
    with_windows
    .filter(F.col("attr_key").isNotNull() & ~F.col("attr_key").isin(["region", "trace"]))
    .withColumn("seq_no_per_cumulative_status_code", F.round((F.col("seq_no") / F.col("cumulative_status_code")), 2))
    .withColumn("log_ts_date", F.to_date("log_ts"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("N/A")))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w8).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w8) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = (
    gaps
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        "first_known_attr_key",
        "service_stddev_samp_status_code_share",
        F.sum("session_start_flag").over(w9).alias("session_id"),
    )
)

result = (
    sessions_app_logs
    .groupBy(
        F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
        "level",
        F.when(F.col("service_stddev_samp_status_code_share") >= 0.69, "HIGH").otherwise("LOW").alias("service_stddev_samp_status_code_share_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sum(F.when(F.col("attr_key") == "trace", F.col("status_code")).otherwise(0)).alias("trace_status_code"),
        F.max("log_ts").alias("last_log_ts"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
    )
    .withColumn("trace_status_code_percent_rank", F.percent_rank().over(w10))
)
