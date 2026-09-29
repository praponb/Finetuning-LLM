from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.year("last_restock") >= 2024)

summary = (
    valid_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = summary
