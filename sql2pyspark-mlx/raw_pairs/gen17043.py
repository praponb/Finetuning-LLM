from pyspark.sql import functions as F

flattened = df_app_logs.select("id", "level", F.explode_outer("context").alias("k", "value"))

remaining_keys = (
    flattened
    .select("level", "id")
    .intersect(
        df_app_logs
        .filter(F.hour("log_ts").between(10, 16))
        .select("level", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("k").isin(["version", "region", "trace"]))
    .withColumn("k_level_key", F.concat_ws(",", "k", "level"))
    .withColumn("k_prefix", F.substring("k", 1, 3))
)

matched_keys = (
    valid
    .select("level", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level").isin(["INFO", "ERROR", "DEBUG"]))
        .select("level", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("k").isNotNull())
    .withColumn("k_group", F.when(F.col("k").isin(["version", "region"]), "Group A").otherwise("Other"))
)

cleaned = (
    base
    .filter((~F.col("k").eqNullSafe(F.lit("version"))) & (F.col("level") != "DEBUG"))
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

result = (
    cleaned
    .select("id", "k", "k_level_key", "clean_level")
    .orderBy(F.col("id").desc_nulls_first(), F.col("k").desc())
)
