from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("store_id").isin([2, 3]) & (F.col("store_id") == 5))

agg = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    agg
    .select("store_id", "total_quantity")
    .orderBy("total_quantity", "store_id")
    .limit(5)
)
