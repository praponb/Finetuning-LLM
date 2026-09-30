from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 11]))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
