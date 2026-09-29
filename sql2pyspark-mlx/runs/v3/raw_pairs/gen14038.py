from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.col("last_restock") < "2023-04-01") & F.col("store_id").isin([1, 2, 4]))
)

inventory_stats = (
    filtered_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("inventory_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    inventory_stats
    .filter(F.col("inventory_count") > 5)
    .select("product_id", "inventory_count", "latest_last_restock")
)
