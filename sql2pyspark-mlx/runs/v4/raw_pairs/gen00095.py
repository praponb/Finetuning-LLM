from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

flattened = exploded.select("id", "key", F.explode("coupon_codes").alias("tag"))

cleaned = (
    flattened
    .filter(F.col("tag").isNotNull())
    .withColumn("tag_title", F.initcap(F.trim("tag")))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("OTHER")))
)

filtered = (
    cleaned
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.concat("key", F.lit(", "), "tag").alias("key_tag"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("tag_clean_2"),
    )
)

base = (
    filtered
    .filter(F.col("key") == "utm_source")
    .select(
        "key",
        "tag_title",
        "tag_clean",
        "key_tag",
        "tag_clean_2",
        F.substring("tag", 1, 3).alias("tag_prefix"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled"),
    )
)

filtered_carts = (
    base
    .filter(F.col("key").isNotNull() & F.col("key").isin(["weight_kg", "utm_source", "gift"]))
    .withColumn("key_prefix", F.substring("key", 1, 3))
    .withColumn("key_clean", F.coalesce("key", F.lit("NONE")))
)

filtered_carts_2 = (
    filtered_carts
    .filter(F.col("key") == "gift")
    .select(
        "*",
        F.substring("key", 1, 3).alias("key_prefix_2"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

result = (
    filtered_carts_2
    .select(
        "tag_clean",
        "key_tag",
        "tag_clean_2",
        "tag_prefix",
        "tag_filled",
        "key_prefix",
        "key_clean",
        "key_prefix_2",
        "resolved_key",
    )
)
