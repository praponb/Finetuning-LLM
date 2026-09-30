from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("key")
w3 = Window.orderBy("rank_in_group", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

latest = (
    unpacked
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "latency_ms",
        "key",
        "status_code_high",
        F.max("latency_ms").over(w2).alias("key_max_latency_ms"),
        F.first("status_code_high", ignorenulls=True).over(w3).alias("first_known_status_code_high"),
        F.ntile(5).over(w4).alias("latency_ms_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        "key",
        F.when(F.col("latency_ms_bucket") >= 8, "LARGE").otherwise("SMALL").alias("latency_ms_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("key") == "region", F.col("first_known_status_code_high")).otherwise(0)).alias("region_first_known_status_code_high"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count_2"),
    )
    .filter(F.col("region_first_known_status_code_high") > 0)
)
