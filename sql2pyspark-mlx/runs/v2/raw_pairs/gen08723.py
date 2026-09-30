from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 7)
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
