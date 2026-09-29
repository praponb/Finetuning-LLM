from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-04-01")
    .select("id", "last_restock", "product_id", "store_id")
    .orderBy(F.col("last_restock").desc())
)
