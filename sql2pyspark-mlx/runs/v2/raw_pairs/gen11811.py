from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-02-09", "2025-08-01") & (F.year("last_restock") == 2023))
    .select("id", "quantity", "last_restock", "product_id")
    .orderBy(F.col("quantity").desc(), "id")
    .limit(5)
)
