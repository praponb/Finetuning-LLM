from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

cleaned = (
    flattened_carts
    .filter((F.col("value").eqNullSafe(F.lit("VIP"))) & (F.col("promo") == "BUNDLE"))
    .select(
        "*",
        F.trim("value").alias("clean_value"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-02 01:00:00").cast("timestamp"))
    .select(
        "created_at",
        "price",
        "promo",
        "value",
        "clean_value",
        "promo_filled",
        (F.col("qty") - F.col("price")).alias("qty_minus_price"),
    )
)

result = (
    base
    .select(
        "created_at",
        "promo",
        "value",
        "clean_value",
        "promo_filled",
        "qty_minus_price",
        F.when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "GROUP_B").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(F.col("value").desc_nulls_last())
)
