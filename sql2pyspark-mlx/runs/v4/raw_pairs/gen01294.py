from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

parsed = (
    flattened
    .filter(F.col("attr_key") == "version")
    .select(
        "id",
        "attr_key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

matched_keys = (
    parsed
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").isNotNull())
        .select("id")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("attr_key") == "version")
    .withColumn("status_code_high_scaled", F.round((F.col("status_code_high") * 1.07), 2))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("N/A")))
)

result = (
    valid
    .groupBy(
        "attr_key",
        F.when(F.col("flag_bit2") >= 1, "HIGH").otherwise("LOW").alias("flag_bit2_tier"),
        F.when(F.col("status_code_high_scaled") >= 61, "LARGE").otherwise("SMALL").alias("status_code_high_scaled_tier"),
    )
    .agg(
        F.max(F.when(F.col("attr_key") == "version", F.col("status_code_high"))).alias("version_max_status_code_high"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.round(F.sum("status_code_high_scaled") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("status_code_high_scaled_per_trace"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .orderBy(F.col("distinct_attr_keys").asc_nulls_last())
)
