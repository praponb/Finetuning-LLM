from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

cleaned = (
    exploded
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        (F.col("qty") - F.col("product_id")).alias("qty_minus_product_id"),
        F.array_contains("coupon_codes", "VIP").alias("is_vip"),
        F.coalesce("promo", F.lit("OTHER")).alias("resolved_promo"),
    )
)

base = (
    cleaned
    .filter(F.col("promo").eqNullSafe(F.lit("VIP")))
    .withColumn("qty_num", F.col("qty").cast("string"))
    .withColumn("coupon_codes_text", F.array_join("coupon_codes", ", "))
)

filtered = (
    base
    .filter(F.col("price") >= 140)
    .select(
        "*",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

result = (
    filtered
    .select(
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        "qty_minus_product_id",
        "is_vip",
        "qty_num",
        "coupon_codes_text",
    )
    .orderBy(F.col("product_id").desc(), "qty_minus_product_id")
)
