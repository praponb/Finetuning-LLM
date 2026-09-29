from pyspark.sql import functions as F

min_last_restock = df_inventory.filter(F.col("last_restock").between("2023-12-01", "2024-02-01")).agg(F.min("last_restock")).first()[0]

result = (
    df_inventory
    .filter(F.col("last_restock") == min_last_restock)
    .select("id", "last_restock", "product_id")
    .orderBy(F.col("id").desc())
    .limit(5)
)
