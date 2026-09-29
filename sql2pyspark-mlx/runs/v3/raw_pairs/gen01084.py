from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 10]) & F.month("last_restock").isin([6, 8, 9]))
    .cube("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
