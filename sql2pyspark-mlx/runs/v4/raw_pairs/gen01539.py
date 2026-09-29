from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("price") >= 203, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.count("*").alias("cnt"),
        F.sum("product_id").alias("total_product_id"),
    )
)
