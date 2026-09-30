from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 177) & F.col("product_id").isin([3, 5, 8]))
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
