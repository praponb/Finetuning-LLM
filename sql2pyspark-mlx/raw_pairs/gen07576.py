from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "level", F.explode_outer("context").alias("key", "value"))
)

valid = (
    lines
    .filter(~F.col("key").eqNullSafe(F.lit("version")))
    .withColumn("clean_key", F.trim("key"))
)

matched_keys = (
    valid
    .select("level", "service")
    .subtract(
        df_app_logs
        .filter(F.col("level") == "DEBUG")
        .select("level", "service")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("level") == "DEBUG")
    .select(
        "*",
        F.substring("key", 1, 2).alias("key_prefix"),
        F.upper("key").alias("key_upper"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

matched_keys_2 = (
    cleaned
    .select("level")
    .intersect(
        df_app_logs
        .filter(F.col("host") == "h1")
        .select("level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "service",
        "key",
        "clean_key",
        "key_prefix",
        "key_upper",
        "key_filled",
    )
)
