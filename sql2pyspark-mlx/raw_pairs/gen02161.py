from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "v"))
)

valid = (
    exploded
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.lower("attr_key").alias("attr_key_lower"),
    )
)

valid_carts = (
    valid
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "*",
        F.trim("attr_key").alias("clean_attr_key"),
        F.when(F.col("attr_key").isin(["device", "weight_kg"]), "Priority").otherwise("Other").alias("attr_key_group"),
    )
)

filtered = (
    valid_carts
    .filter(F.col("attr_key").isNull())
    .select(
        "id",
        "coupon_codes",
        "attr_key",
        "attr_key_title",
        "clean_attr_key",
        "attr_key_group",
        F.trim("attr_key").alias("clean_attr_key_2"),
    )
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull() & (F.size("coupon_codes") > 1))
    .withColumn("attr_key_upper", F.upper("attr_key"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("NONE")))
)

base_carts = (
    base
    .filter(F.col("attr_key").isNotNull() & (F.size("coupon_codes") > 1))
    .select(
        "id",
        "coupon_codes",
        "attr_key",
        "clean_attr_key",
        "attr_key_group",
        "clean_attr_key_2",
        "attr_key_upper",
        "resolved_attr_key",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

result = (
    base_carts
    .select(
        "id",
        "attr_key",
        "clean_attr_key",
        "attr_key_group",
        "attr_key_upper",
        "resolved_attr_key",
        "attr_key_clean",
    )
    .orderBy(F.col("attr_key").desc())
)
