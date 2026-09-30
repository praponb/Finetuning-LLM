from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5, 6]) & (F.col("last_restock").between("2024-07-25", "2024-12-29") | (F.col("last_restock") < "2024-12-09")))
    .select("id", "product_id", "quantity", "store_id")
    .orderBy(F.col("quantity").desc())
)
