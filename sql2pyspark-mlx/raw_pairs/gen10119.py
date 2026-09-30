from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w2 = Window.partitionBy("key").orderBy(F.col("has_fault").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "host",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .filter(F.col("has_fault") < 1)
    .withColumn("status_code_percent_rank", F.percent_rank().over(w))
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w2))
)

valid = (
    enriched
    .filter(F.col("key").isNotNull())
    .withColumn("has_fault_cume_dist_per_status_code_percent_rank", (F.col("has_fault_cume_dist") / F.nullif("status_code_percent_rank", F.lit(0))))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

result = (
    valid
    .groupBy("key")
    .agg(
        F.sum(F.when(F.col("key") == "trace", F.col("status_code_high")).otherwise(0)).alias("trace_status_code_high"),
        F.sum(F.when(F.col("key") == "version", F.col("status_code_percent_rank")).otherwise(0)).alias("version_status_code_percent_rank"),
        F.count_if(F.col("key") == "version").alias("version_events"),
    )
    .orderBy("trace_status_code_high")
)
