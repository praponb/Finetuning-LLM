from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("max_item_price") >= 262, "HIGH").otherwise("LOW").alias("max_item_price_tier"),
    )
    .agg(
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles_2"),
    )
    .filter(F.col("avg_max_item_price") > 0)
)
