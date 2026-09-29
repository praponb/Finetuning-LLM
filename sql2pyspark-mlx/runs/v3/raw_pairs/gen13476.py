from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter((F.col("quantity") > 74) | F.col("product_id").isin([8, 10]))

inventory_stats = (
    filtered_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = inventory_stats.filter(F.col("unique_stores") > 9)
