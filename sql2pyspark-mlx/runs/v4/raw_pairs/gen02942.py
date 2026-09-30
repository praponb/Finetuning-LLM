from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("k").isNotNull())
    .select(
        "service",
        "latency_ms",
        "k",
        "has_fault",
        "status_code_high",
        F.when(F.col("k").isin(["region", "version"]), "Core").otherwise("Other").alias("k_group"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

pivoted = (
    valid
    .groupBy("service")
    .pivot("k", ["trace", "region", "version"])
    .agg(F.sum("latency_ms").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)

result = (
    pivoted
    .select(
        "service",
        "trace_value_sum",
        "region_item_count",
        "version_value_sum",
        "version_item_count",
        F.when(F.col("service").rlike("^(checkout|billing)$"), "GROUP_B").when(F.col("service").rlike("^(auth|search)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("service_zone"),
    )
    .orderBy(F.col("service").desc_nulls_first())
)
