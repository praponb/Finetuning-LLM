from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 5) & (F.col("quantity") < 207))
    .select("id", "quantity", "last_restock", "store_id")
    .orderBy(F.col("id").desc())
)
