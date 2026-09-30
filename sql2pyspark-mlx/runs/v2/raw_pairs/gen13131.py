from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.col("product_id").isin([5, 6, 7, 10]) & (F.col("store_id") == 2)) | (F.col("last_restock") <= "2025-02-01"))
)

inventory_stats = (
    filtered_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    inventory_stats
    .filter(F.col("quantity_range") >= 50)
    .select("last_restock_month", "quantity_range")
    .orderBy("quantity_range", "last_restock_month")
    .limit(10)
)
