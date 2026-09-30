from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.min("created_at").alias("started_at"),
        F.count("*").alias("event_count"),
    )
)
