from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([7, 12, 23]))
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 18)
)
