from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("has_fault").desc_nulls_last())
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy(F.col("has_fault").desc())
w4 = Window.orderBy(F.col("has_fault").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

parsed = (
    flattened
    .select(
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

valid = (
    parsed
    .filter(F.col("k").isNotNull() & F.col("status_code").between(268, 425))
    .select(
        "k",
        "v",
        "has_fault",
        F.round((F.col("latency_ms") * 0.9), 2).alias("latency_ms_adjusted"),
        F.substring("k", 1, 4).alias("k_prefix"),
    )
)

with_windows = (
    valid
    .filter(F.col("k") == "trace")
    .withColumn("has_fault_dense_rank", F.dense_rank().over(w))
    .withColumn("k_avg_has_fault", F.avg("has_fault").over(w2))
    .withColumn("has_fault_rank", F.rank().over(w3))
    .withColumn("has_fault_percent_rank", F.percent_rank().over(w4))
)

result = (
    with_windows
    .cube("k")
    .agg(
        F.sum(F.when(F.col("k") == "region", F.col("has_fault_percent_rank")).otherwise(0)).alias("region_has_fault_percent_rank"),
        F.avg(F.when(F.col("k") == "version", F.col("has_fault_dense_rank"))).alias("version_avg_has_fault_dense_rank"),
        F.sum(F.when(F.col("k") == "region", F.col("k_avg_has_fault")).otherwise(0)).alias("region_k_avg_has_fault"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("region_k_avg_has_fault") < 1000000)
    .orderBy(F.col("region_k_avg_has_fault").desc_nulls_last())
)
