from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]) & F.col("quantity").between(203, 226))
    .select("id", "store_id", "last_restock")
)
