from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "level",
        "attr_key",
        "value",
        F.when(F.col("host").isin(["h3", "h1"]), "Core").otherwise("Other").alias("host_group"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_filled"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") == "version"))
    .select(
        "id",
        "level",
        "attr_key",
        F.substring("attr_key", 1, 4).alias("attr_key_prefix"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

base = (
    filtered_app_logs
    .filter(F.col("attr_key").isin(["version", "trace"]))
    .select(
        "id",
        "attr_key",
        "attr_key_prefix",
        "attr_key_clean",
        F.lower("attr_key").alias("attr_key_lower"),
    )
)

filtered_app_logs_2 = (
    base
    .filter(F.col("attr_key") == "region")
    .select(
        "id",
        "attr_key",
        "attr_key_prefix",
        "attr_key_clean",
        "attr_key_lower",
        F.lower("attr_key").alias("attr_key_lower_2"),
    )
)

result = (
    filtered_app_logs_2
    .select("attr_key", "attr_key_clean", "attr_key_lower")
    .orderBy(F.col("attr_key").desc_nulls_first())
)
