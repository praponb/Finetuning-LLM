from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

base = (
    lines
    .filter(F.col("attr_key").isNotNull() & (F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))))
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.substring("attr_key", 1, 4).alias("attr_key_prefix"),
    )
)

cleaned = (
    base
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 16:00:00")))
    .withColumn("attr_key_prefix_2", F.substring("attr_key", 1, 3))
)

valid = (
    cleaned
    .filter((F.col("created_at") >= F.lit("2025-03-01 15:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "attr_key",
        "attr_key_upper",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.substring("attr_key", 1, 3).alias("attr_key_prefix_3"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("resolved_attr_key"),
    )
)

base_carts = (
    valid
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "attr_key",
        "coupon_codes_count",
        "resolved_attr_key",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.array_contains("coupon_codes", "VIP").alias("is_vip"),
    )
)

result = (
    base_carts
    .select(
        "attr_key",
        "coupon_codes_count",
        "resolved_attr_key",
        "sorted_coupon_codes",
        "is_vip",
        F.when(F.col("attr_key").rlike("^(weight_kg|utm_source)$"), "GROUP_B").when(F.col("attr_key").rlike("^(device|gift)$"), "GROUP_A").otherwise("CORE").alias("attr_key_zone"),
    )
    .orderBy(F.col("coupon_codes_count").desc())
)
