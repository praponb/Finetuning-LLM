from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") >= 1)
    .orderBy("last_restock_month")
    .limit(20)
)
