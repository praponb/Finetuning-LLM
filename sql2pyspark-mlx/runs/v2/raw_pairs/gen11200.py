from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 1230)
)
