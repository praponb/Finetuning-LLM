from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 200, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .filter(F.col("min_quantity") > 6.8)
    .orderBy(F.col("min_quantity").desc())
)
