from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 1)
    .drop("unique_products")
)
