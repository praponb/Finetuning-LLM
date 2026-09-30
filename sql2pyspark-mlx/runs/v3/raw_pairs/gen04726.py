from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 6)
    .select("last_restock_quarter", "store_id", "quantity_count")
    .orderBy("last_restock_quarter")
)
