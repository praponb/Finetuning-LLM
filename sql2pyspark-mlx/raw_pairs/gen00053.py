from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

lines = (
    exploded
    .select(
        "customer_id",
        "qty",
        "price",
        "promo",
        F.explode("coupon_codes").alias("tag"),
    )
)

base = (
    lines
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("tag").alias("tag_upper"),
        F.round("price", 2).alias("price_rounded"),
    )
)

base_carts = (
    base
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.when(F.col("price_rounded") >= 90, "High").when(F.col("price_rounded") >= 40, "Medium").otherwise("Low").alias("price_rounded_level"),
        F.coalesce("promo", F.lit("N/A")).alias("resolved_promo"),
    )
)

summary = (
    base_carts
    .groupBy(
        F.when(F.col("price_rounded") >= 69, "PREMIUM").otherwise("STANDARD").alias("price_rounded_tier"),
        "promo",
    )
    .agg(
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("tag") == "SPRING10", 1)), F.lit(0)), 2).alias("qty_per_spring10"),
        F.sum(F.when(F.col("tag") == "WELCOME5", F.col("qty"))).alias("welcome5_total_qty"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
    )
    .filter(F.col("qty_per_spring10") >= 0)
)

result = (
    summary
    .select(
        "price_rounded_tier",
        "promo",
        "qty_per_spring10",
        "welcome5_total_qty",
        "promo_list",
    )
)
