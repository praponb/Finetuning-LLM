from pyspark.sql import functions as F

valid_inventory = df_inventory.filter((F.col("store_id") == 5) & (F.col("product_id") == 4))

store_id_summary = (
    valid_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = store_id_summary
