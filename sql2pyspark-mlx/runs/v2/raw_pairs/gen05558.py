from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-01", "2025-04-01") & F.col("quantity").isNotNull() & F.col("product_id").isin([1, 2, 8]))
    .withColumn("quantity_whole", F.floor("quantity"))
    .withColumn("quantity_rounded", F.round("quantity", 0))
    .orderBy("store_id")
)
