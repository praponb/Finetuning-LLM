from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .groupBy(F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"))
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
