from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([2, 3, 7, 10]) & F.col("last_restock").between("2024-01-05", "2024-10-01")))
    .select("id", "store_id", "last_restock", "product_id")
)
