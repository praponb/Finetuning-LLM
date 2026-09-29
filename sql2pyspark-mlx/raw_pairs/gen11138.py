from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("quantity") != 188)

inventory_stats = (
    filtered_inventory
    .groupBy(
        F.when(F.col("quantity") >= 228, "high").when(F.col("quantity") >= 100, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("total_quantity") > 1180)
    .select("quantity_bucket", "product_id", "total_quantity", "latest_last_restock")
)
