from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["gift"].alias("gift"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_items") >= 3, "PREMIUM").otherwise("STANDARD").alias("num_items_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("num_items").alias("max_num_items"),
    )
    .orderBy("num_items_tier")
)
