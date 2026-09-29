from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-05-18", "2024-10-16"))
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
    .select("product_id", "quantity_count")
    .orderBy(F.col("product_id").desc())
)
