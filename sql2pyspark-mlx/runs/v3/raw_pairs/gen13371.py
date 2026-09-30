from pyspark.sql import functions as F

src = df_inventory.filter(F.col("product_id").isin([4, 5, 8, 9]))

inventory_stats = (
    src
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = inventory_stats.orderBy(F.col("last_restock_month").desc())
