from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("store_id").isin([2, 4, 6]) & (F.col("product_id") == 3)) | (F.col("quantity") > 200))
)

summary = (
    base
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    summary
    .select("product_id", "last_restock_month")
    .orderBy("product_id")
)
