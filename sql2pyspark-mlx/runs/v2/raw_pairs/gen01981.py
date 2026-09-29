from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 6, 9]) & F.col("store_id").isin([2, 4]))
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .orderBy(F.col("product_id").desc())
)
