from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("inventory_count"),
        F.min("last_restock").alias("first_last_restock"),
    )
)
