from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

lines_carts = (
    lines
    .select("id", "attr_key", "attr_value", F.explode("coupon_codes").alias("value"))
)

base = (
    lines_carts
    .filter(F.col("value").isin(["WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("value").isin(["SPRING10", "BUNDLE"]), "Tier 1").otherwise("Other").alias("value_group"),
        F.when(F.col("attr_key").isin(["gift", "utm_source"]), "Tier 1").otherwise("Other").alias("attr_key_group"),
    )
)

valid = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "attr_value",
        "value",
        "value_group",
        F.when(F.col("attr_key").isin(["weight_kg", "device"]), "Core").otherwise("Other").alias("attr_key_group_2"),
        F.trim("value").alias("clean_value"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_clean"),
    )
)

base_carts = (
    valid
    .filter(F.col("value") == "BUNDLE")
    .select(
        "attr_value",
        "value",
        "value_group",
        "attr_key_group_2",
        "value_clean",
        F.coalesce("value", F.lit("Unknown")).alias("value_filled"),
    )
)

result = (
    base_carts
    .select(
        "attr_value",
        "value",
        "value_group",
        "attr_key_group_2",
        "value_clean",
        "value_filled",
    )
)
