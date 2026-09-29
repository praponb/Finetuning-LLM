from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]) & (F.col("store_id") == 6) & F.col("store_id").isin([1, 2, 3, 5]))
)

inventory_stats = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    inventory_stats
    .select("product_id", "store_id")
    .orderBy("product_id", "store_id")
    .limit(20)
)
