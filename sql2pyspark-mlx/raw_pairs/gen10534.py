from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)

result = (
    flattened_attrs
    .rollup(
        F.when(F.col("max_item_price") >= 213, "HIGH").otherwise("LOW").alias("max_item_price_tier"),
    )
    .agg(
        F.max("max_item_price").alias("max_max_item_price"),
        F.percentile_approx("max_item_price", 0.95).alias("max_item_price_p95"),
        F.count("*").alias("num_rows"),
    )
)
