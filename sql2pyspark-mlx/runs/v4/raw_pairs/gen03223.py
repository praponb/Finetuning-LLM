from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") >= 2024) & F.col("product_id").isin([1, 8]))
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .orderBy(F.col("quantity").desc())
)
