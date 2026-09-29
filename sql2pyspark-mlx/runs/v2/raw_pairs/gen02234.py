from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 8, 10]) & F.col("product_id").isin([3, 4, 5, 9]))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)
