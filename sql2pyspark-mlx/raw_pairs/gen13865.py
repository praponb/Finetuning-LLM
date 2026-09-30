from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
    )
)
