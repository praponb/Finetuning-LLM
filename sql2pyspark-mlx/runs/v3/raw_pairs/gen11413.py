from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("last_restock").between("2023-05-01", "2024-02-16"))

agg = (
    filtered_inventory
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    agg
    .filter(F.col("min_quantity") >= 150)
    .orderBy(F.col("product_id").desc())
)
