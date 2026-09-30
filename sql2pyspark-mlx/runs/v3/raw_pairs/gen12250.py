from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5, 6]) & F.col("quantity").isNull() & F.month("last_restock").isin([2, 4, 8]))
)

product_id_summary = (
    valid_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = product_id_summary.select("product_id", "last_restock_month", "last_last_restock")
