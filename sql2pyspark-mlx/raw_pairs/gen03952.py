from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
