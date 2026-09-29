from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") < 170)
    .select("product_id", "unit_price_bucket", "avg_unit_price", "min_unit_price")
    .orderBy(F.col("min_unit_price").desc(), "unit_price_bucket")
)
