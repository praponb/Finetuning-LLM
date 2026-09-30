from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 6]))
    .select("id", "store_id", "last_restock", "product_id")
)
