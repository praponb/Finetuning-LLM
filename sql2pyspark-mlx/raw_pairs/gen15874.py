from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

base = (
    flattened
    .filter(F.col("promo").isNotNull() & (F.col("price") < 80))
    .select(
        "*",
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
        F.col("qty").cast("double").alias("qty_num"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_clean"),
    )
)

filtered = (
    base
    .filter(F.col("promo").isNotNull())
    .select(
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "coupon_codes_text",
        "qty_num",
        "promo_clean",
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
    )
)

base_carts = (
    filtered
    .filter(F.col("qty_num") > 65)
    .select(
        "qty",
        "promo_clean",
        "promo_filled",
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("promo", F.lit("OTHER")).alias("resolved_promo"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.col("qty").between(3, 4))
    .select(
        "*",
        (F.col("price_int") - F.col("qty")).alias("price_int_minus_qty"),
        F.round((F.col("price_int") / F.col("qty")), 2).alias("price_int_per_qty"),
    )
)

result = (
    base_carts_2
    .groupBy(
        F.when(F.col("price_int_per_qty") >= 78, "HIGH").otherwise("LOW").alias("price_int_per_qty_tier"),
        F.when(F.col("price_int_minus_qty") >= 65, "PREMIUM").otherwise("STANDARD").alias("price_int_minus_qty_tier"),
    )
    .agg(
        F.min("qty").alias("min_qty"),
        F.round(F.avg("price_int_minus_qty"), 2).alias("avg_price_int_minus_qty"),
        F.percentile_approx("price_int", 0.95).alias("price_int_p95"),
    )
)
