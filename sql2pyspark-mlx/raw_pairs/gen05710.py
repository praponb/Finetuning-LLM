from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
)
