from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(75, 225) | F.col("product_id").isin([1, 9]))
    .select("store_id", "product_id", F.year("last_restock").alias("last_restock_year"))
    .orderBy(F.col("store_id").desc())
)
