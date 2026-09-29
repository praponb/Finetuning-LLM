from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count_3"),
    )
    .filter(F.col("event_count_3") < 1000)
)
