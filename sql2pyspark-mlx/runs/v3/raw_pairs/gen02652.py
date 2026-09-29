from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(142, 190))
    .select("store_id", "last_restock", "product_id")
)
