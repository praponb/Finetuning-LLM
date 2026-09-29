from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("last_restock") > "2023-06-13")

summary = (
    filtered_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    summary
    .filter(F.col("unique_products") < 8)
    .select("store_id", "unique_products", "avg_quantity")
    .orderBy("avg_quantity")
)
