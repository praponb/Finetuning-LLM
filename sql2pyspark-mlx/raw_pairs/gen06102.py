from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "service",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter(F.col("key").isNotNull())
    .withColumn("status_code_high_num", F.col("status_code_high").cast("double"))
)

base = (
    filtered
    .filter(F.col("key").isin(["trace", "version"]))
    .withColumn("status_code_adjusted", F.round((F.col("status_code") * 0.9), 2))
    .withColumn("has_fault_minus_status_code_high", (F.col("has_fault") - F.col("status_code_high")))
)

totals = (
    base
    .withColumn("has_fault_tier", F.when(F.col("has_fault") >= 1, "LARGE").otherwise("SMALL"))
    .rollup("key", "has_fault_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
        F.count("*").alias("cnt"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.grouping("key").alias("gid"),
    )
)

result = (
    totals
    .select(
        "key",
        "has_fault_tier",
        "distinct_ids",
        "region_rows",
        "cnt",
        "status_code_bit_or",
        "gid",
    )
    .orderBy(F.col("region_rows").desc_nulls_first())
)
