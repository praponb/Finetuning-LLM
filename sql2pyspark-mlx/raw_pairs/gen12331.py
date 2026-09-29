from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
    )
)
