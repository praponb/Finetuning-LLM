from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

flattened = exploded.select("promo", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    flattened
    .filter((~F.col("code").eqNullSafe(F.lit("VIP"))) & F.col("code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .select(
        "*",
        F.concat("promo", F.lit(" "), "code").alias("promo_code"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("code").isNotNull())
    .select(
        "*",
        F.substring("promo", 1, 3).alias("promo_prefix"),
        F.lower("code").alias("code_lower"),
    )
)

filtered = (
    cleaned_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "promo",
        "code",
        "promo_code",
        "promo_filled",
        "code_lower",
        F.trim("promo").alias("clean_promo"),
        F.coalesce("promo", F.lit("N/A")).alias("resolved_promo"),
    )
)

result = (
    filtered
    .groupBy("code")
    .agg(
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_count"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("spring10_count") < 1000)
    .orderBy(F.col("spring10_count").desc_nulls_first())
)
