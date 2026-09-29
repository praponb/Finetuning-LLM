from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(~(F.col("product_id").isin([4, 5, 6, 9]) & F.col("last_restock").between("2023-07-01", "2024-12-03")))
)

inventory_stats = (
    valid_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    inventory_stats
    .filter(F.col("min_quantity") > 160)
    .select("store_id", "product_id", "total_quantity", "min_quantity")
    .orderBy("total_quantity", F.col("min_quantity").desc(), "store_id", "product_id")
    .limit(3)
)
