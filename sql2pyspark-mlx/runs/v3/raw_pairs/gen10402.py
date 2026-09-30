from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("total_unit_price") >= 1000)
)
