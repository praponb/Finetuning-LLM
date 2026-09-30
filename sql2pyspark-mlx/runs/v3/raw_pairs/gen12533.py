from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 17)
    .drop("total_quantity")
)
