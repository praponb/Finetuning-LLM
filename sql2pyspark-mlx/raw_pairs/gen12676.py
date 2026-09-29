from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("attributes").alias("attr_key", "value"))

valid = (
    lines
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"))
)

kept_keys = (
    valid
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") < "2025-01-01")
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("attr_key").isin(["device", "utm_source"]))
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.coalesce("attr_key", F.lit("Unknown")).alias("attr_key_filled_2"),
    )
)

filtered = (
    base
    .filter(F.col("attr_key").isin(["weight_kg", "device", "utm_source"]))
    .select(
        "id",
        "value",
        "attr_key_title",
        "attr_key_filled_2",
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

result = filtered.select("id")
