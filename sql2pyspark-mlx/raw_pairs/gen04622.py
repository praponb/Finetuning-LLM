from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-10-01", "2024-10-25") & F.col("product_id").isin([2, 4, 5, 6]))
    .select("quantity", "last_restock", "product_id")
    .orderBy(F.col("quantity").desc())
)
