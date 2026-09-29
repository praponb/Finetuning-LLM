from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]) & ((F.col("last_restock") < "2023-05-10") | F.col("store_id").isin([4, 6])))
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
