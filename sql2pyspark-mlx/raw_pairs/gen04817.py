from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 9, 10]) & (F.col("quantity") != 194))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 170)
    .drop("avg_quantity")
)
