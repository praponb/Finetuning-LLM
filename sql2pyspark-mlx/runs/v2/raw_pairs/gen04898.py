from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]) & (F.col("last_restock") < "2024-01-16") & (F.col("store_id") == 1))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)

store_id_summary = (
    base
    .groupBy("store_id", "product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = store_id_summary.orderBy("product_id")
