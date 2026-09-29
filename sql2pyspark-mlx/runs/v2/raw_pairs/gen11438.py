from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([1, 22, 39]) & F.col("order_id").isin([5, 22, 40]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("avg_quantity") >= 4.6)
    .orderBy(F.col("product_id").desc())
)
