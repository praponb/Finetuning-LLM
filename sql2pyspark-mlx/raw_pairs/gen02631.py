from pyspark.sql import functions as F

lines = (
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
    )
)

result = (
    lines
    .groupBy(F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"))
    .agg(
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
