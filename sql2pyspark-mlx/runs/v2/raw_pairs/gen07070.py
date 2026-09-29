from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("quantity") < 141)

inventory_stats = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("avg_quantity") > 200)
    .select("product_id", "store_id", "avg_quantity")
    .orderBy("avg_quantity", "product_id", "store_id")
    .limit(5)
)
