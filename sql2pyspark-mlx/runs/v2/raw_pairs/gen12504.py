from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2025-02-01", "2025-08-05") & (F.col("last_restock") < "2024-09-01"))
)

inventory_stats = (
    filtered_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = inventory_stats
