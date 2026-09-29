from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes").getItem("device").alias("device"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)
