from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 3, 8]) | F.col("product_id").isin([1, 2, 4, 7]))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
    .orderBy(F.col("last_restock_plus_months").desc(), "product_id", "id")
    .limit(3)
)
