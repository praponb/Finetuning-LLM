from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 17)
    .groupBy("product_id", "order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("avg_quantity") > 7.3)
)
