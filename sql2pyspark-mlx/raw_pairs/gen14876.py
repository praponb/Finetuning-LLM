from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id").isin([2, 3, 4]))

inventory_stats = (
    filtered_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = inventory_stats.orderBy(F.col("product_id").desc())
