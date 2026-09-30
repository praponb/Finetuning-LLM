from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNull())
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)
