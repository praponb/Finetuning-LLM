from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.count("quantity").alias("quantity_count"))
)
