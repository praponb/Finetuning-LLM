from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.size("attributes").alias("num_attributes"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)
