from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

kept_keys = (
    flattened
    .select("service", "id")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "WARN")
        .select("service", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("attr_key").isNotNull() & (F.col("log_ts") >= "2023-08-01"))
    .select(
        "service",
        "log_ts",
        "attr_key",
        "attr_value",
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

result = (
    valid
    .select(
        "service",
        "log_ts",
        "attr_key",
        "attr_value",
        "attr_key_filled",
        F.when(F.col("service").rlike("^(search|auth)$"), "CORE").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").desc_nulls_last())
)
