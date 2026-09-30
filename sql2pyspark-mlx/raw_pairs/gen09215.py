from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    lines
    .withColumn("price_tier", F.when(F.col("price") >= 219, "LARGE").otherwise("SMALL"))
    .withColumn("product_id_tier", F.when(F.col("product_id") >= 11, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["price_tier", "product_id_tier"], ["price_tier"]],
        "price_tier",
        "product_id_tier",
    )
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("qty")).otherwise(0)).alias("spring10_qty"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
