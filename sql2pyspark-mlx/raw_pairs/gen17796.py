from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 8)
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.min("quantity").alias("min_quantity"),
    )
)
