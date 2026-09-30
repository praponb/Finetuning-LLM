from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .cube(
        "customer_id",
        F.when(F.col("weight_kg") >= 3.9, "PREMIUM").otherwise("STANDARD").alias("weight_kg_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("duration_min") < 1000000)
)
