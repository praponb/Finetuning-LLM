from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4]) & F.col("product_id").isin([1, 9]) & (F.col("product_id") == 6))
)

summary = (
    base
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = summary.orderBy("product_id")
