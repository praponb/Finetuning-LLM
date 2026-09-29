from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"), "level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("week", "log_ts"), "service").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key", "level").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_last())
w6 = Window.partitionBy("attr_key", "service").orderBy(F.col("latency_ms_percent_rank").desc_nulls_last(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

base = (
    exploded
    .filter(F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 07:00:00")))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        F.concat_ws("-", "service", "attr_key").alias("service_attr_key_key"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

metrics = (
    base
    .filter(F.to_date("log_ts") < "2024-06-22")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "service_attr_key_key",
        F.sum("latency_ms").over(w).alias("running_latency_ms"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
        F.row_number().over(w3).alias("event_seq"),
        F.corr("latency_ms", "status_code").over(w4).alias("corr_latency_ms_status_code"),
        F.percent_rank().over(w5).alias("latency_ms_percent_rank"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

filtered = (
    deduped
    .filter(F.col("attr_key").isNotNull() & (F.col("latency_ms") <= 2032))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "attr_key",
        "value",
        "service_attr_key_key",
        "running_latency_ms",
        "last_known_attr_key",
        "corr_latency_ms_status_code",
        "latency_ms_percent_rank",
        "row_num",
        F.year("log_ts").alias("log_ts_year"),
    )
)

result = (
    filtered
    .groupBy("service", "level")
    .pivot("attr_key", ["region", "trace", "version"])
    .agg(F.max("latency_ms_percent_rank"))
)
