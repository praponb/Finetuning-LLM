from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("k") == "weight_kg")
    .select(
        "*",
        F.when(F.col("k").isin(["device", "weight_kg"]), "Group A").otherwise("Other").alias("k_group"),
    )
)

filtered = (
    valid
    .filter(F.col("k") == "device")
    .select(
        "*",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

valid_carts = (
    filtered
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "coupon_codes",
        "k",
        "attr_value",
        "sorted_coupon_codes",
        F.when(F.col("k") == "gift", 1).otherwise(0).alias("is_gift"),
    )
)

result = (
    valid_carts
    .select(
        "coupon_codes",
        "k",
        "attr_value",
        F.when(F.col("k").rlike("^(utm_source|device)$"), "CORE").when(F.col("k").rlike("^(gift|weight_kg)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k").desc())
)
