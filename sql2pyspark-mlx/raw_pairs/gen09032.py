from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("attributes")["device"].alias("device"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_attributes") >= 72, "PREMIUM").otherwise("STANDARD").alias("num_attributes_tier"),
    )
    .agg(
        F.min("created_at").alias("started_at"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
    )
    .orderBy("avg_num_attributes")
)
