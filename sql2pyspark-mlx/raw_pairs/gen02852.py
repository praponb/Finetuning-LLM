from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .withColumn("attr_key_count_is_flagged", F.count("is_flagged").over(w))
    .withColumn("attr_key_avg_is_flagged", F.avg("is_flagged").over(w))
)

scored = (
    with_windows
    .filter(F.col("attr_key") != "version")
    .withColumn("attr_key_count_latency_ms", F.count("latency_ms").over(w))
)

base = (
    scored
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_avg_is_flagged_rounded", F.round("attr_key_avg_is_flagged", 0))
    .withColumn("attr_key_count_latency_ms_tier", F.when(F.col("attr_key_count_latency_ms") >= 16, "High").when(F.col("attr_key_count_latency_ms") >= 4, "Medium").otherwise("Low"))
)

result = (
    base
    .groupBy(
        F.when(F.col("latency_ms") >= 2074, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.round(F.avg("is_flagged"), 2).alias("avg_is_flagged"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.avg("status_code_high").alias("avg_status_code_high"),
    )
    .filter(F.col("trace_events") < 1000)
    .orderBy(F.col("version_events").asc_nulls_last())
)
