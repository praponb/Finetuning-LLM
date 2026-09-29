from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .groupBy(
        "promo",
        F.when(F.col("product_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
        F.when(F.col("qty") >= 4, "HIGH").otherwise("LOW").alias("qty_tier"),
    )
    .agg(
        F.percentile_approx("product_id", 0.95).alias("product_id_p95"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("price")).otherwise(0)).alias("vip_price"),
    )
    .orderBy("product_id_tier")
)
