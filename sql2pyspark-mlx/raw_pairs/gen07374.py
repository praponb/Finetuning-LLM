from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 3)
    .select("last_restock_quarter", "row_count")
)
