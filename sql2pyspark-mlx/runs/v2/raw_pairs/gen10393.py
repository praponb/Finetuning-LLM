from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 2)
    .drop("unique_orders")
)
