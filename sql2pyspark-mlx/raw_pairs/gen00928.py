from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "v"))
)

flattened = lines.select("id", "attr_key", "v", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    flattened
    .filter(F.col("code") == "BUNDLE")
    .select("*", F.concat_ws(",", "code", "attr_key").alias("code_attr_key_key"))
)

filtered = (
    cleaned
    .filter(F.col("code") == "WELCOME5")
    .select(
        "id",
        "attr_key",
        "v",
        "code_attr_key_key",
        F.lower("attr_key").alias("attr_key_lower"),
        F.concat_ws("-", "attr_key", "code").alias("attr_key_code_key"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_filled"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "attr_key",
        "v",
        "code_attr_key_key",
        "attr_key_lower",
        "attr_key_code_key",
        "attr_key_filled",
        F.when(F.col("attr_key").rlike("^(device|gift)$"), "EXTENDED").otherwise("PRIORITY").alias("attr_key_zone"),
    )
)
