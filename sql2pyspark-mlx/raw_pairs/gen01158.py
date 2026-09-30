from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "items",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.size("items").alias("num_items"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("num_items") >= 3, "PREMIUM").otherwise("STANDARD").alias("num_items_tier"),
        F.when(F.col("max_item_price") >= 205, "PREMIUM").otherwise("STANDARD").alias("max_item_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("max_item_price").alias("total_max_item_price"),
        F.max("max_item_price").alias("max_max_item_price"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("total_max_item_price") >= 0)
    .orderBy("num_items_tier")
)
