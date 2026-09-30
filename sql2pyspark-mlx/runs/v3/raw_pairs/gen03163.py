from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.month("last_restock").isin([1, 5, 6]))

inventory_stats = (
    recent_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    inventory_stats
    .filter(F.col("quantity_range") > 63)
    .select("store_id", "min_quantity", "quantity_range")
    .orderBy("min_quantity")
)
