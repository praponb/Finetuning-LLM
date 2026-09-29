from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5, 6]))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("total_quantity") > 970)
)
