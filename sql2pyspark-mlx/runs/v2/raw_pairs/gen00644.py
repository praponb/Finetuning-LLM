from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2025) & F.col("store_id").isin([1, 2, 3, 5]) & (F.col("product_id") == 7))
    .orderBy(F.col("quantity").desc(), "id")
    .limit(20)
)
