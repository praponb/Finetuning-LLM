from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

cleaned = (
    flattened
    .filter(F.col("pos") <= 3)
    .select(
        "*",
        F.when(F.col("price") >= 206, "High").when(F.col("price") >= 41, "Medium").otherwise("Low").alias("price_tier"),
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.coalesce("promo", F.lit("unassigned")).alias("promo_filled_2"),
        F.when(F.col("created_at_hour") >= 74, "High").when(F.col("created_at_hour") >= 14, "Medium").otherwise("Low").alias("created_at_hour_band"),
    )
)

cleaned_carts_2 = (
    cleaned_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "created_at",
        "pos",
        "qty",
        "price",
        "promo",
        "price_tier",
        "created_at_hour",
        "promo_filled",
        "promo_filled_2",
        "created_at_hour_band",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.greatest("qty", "price").alias("greatest_qty_price"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

result = (
    cleaned_carts_2
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("greatest_qty_price")).otherwise(0)).alias("bundle_greatest_qty_price"),
        F.round(F.avg("coupon_codes_count"), 2).alias("avg_coupon_codes_count"),
    )
    .filter(F.col("avg_coupon_codes_count") < 1000000)
    .orderBy(F.col("avg_coupon_codes_count").desc_nulls_first())
)
