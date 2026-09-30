from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([5, 9]) & F.col("store_id").isin([1, 2, 4]))
)

agg = (
    base
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "store_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = agg.select("last_restock_quarter", "store_id")
