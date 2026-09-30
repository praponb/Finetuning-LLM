from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("quantity").isNotNull())

inventory_stats = (
    filtered_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("avg_quantity") > 32)
    .orderBy("avg_quantity")
)
