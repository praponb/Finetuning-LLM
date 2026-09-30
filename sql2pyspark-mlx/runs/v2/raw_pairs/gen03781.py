from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") < 154) & F.col("store_id").isin([2, 6]))
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy(F.col("last_last_restock").desc(), "product_id")
)
