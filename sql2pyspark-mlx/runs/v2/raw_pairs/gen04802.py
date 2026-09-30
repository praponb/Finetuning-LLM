from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 30)
    .groupBy(
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc())
)
