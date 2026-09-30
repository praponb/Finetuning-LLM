from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("ship_to.country").alias("ship_country"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.size("attributes").alias("num_attributes"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_attributes") >= 63, "HIGH").otherwise("LOW").alias("num_attributes_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.max("num_attributes").alias("max_num_attributes"),
        F.count(F.when(F.col("ship_country") == "DE", 1)).alias("de_rows"),
    )
    .filter(F.col("de_rows") > 1)
    .orderBy("num_attributes_tier")
)
