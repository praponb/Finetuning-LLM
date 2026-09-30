from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("last_restock") < "2024-07-26") & F.col("store_id").isin([1, 2, 4]) & (F.col("quantity") >= 148))
)

agg = (
    valid_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("inventory_count"))
)

result = (
    agg
    .filter(F.col("inventory_count") < 8)
    .orderBy(F.col("product_id").desc())
    .limit(10)
)
