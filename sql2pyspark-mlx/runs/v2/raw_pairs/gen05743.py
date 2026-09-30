from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 191)
    .select("id", "last_restock", "store_id", "quantity")
    .orderBy("store_id", "id")
    .limit(10)
)
