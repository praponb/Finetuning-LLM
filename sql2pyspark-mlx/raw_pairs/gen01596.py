from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.year("last_restock") == 2025)

agg = (
    valid_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    agg
    .select("store_id", "avg_quantity", "quantity_range")
    .orderBy("avg_quantity", F.col("store_id").desc())
)
