from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 6]) | (F.col("product_id") == 5))
    .select("id", "store_id", "last_restock", "product_id")
    .orderBy("store_id")
)
