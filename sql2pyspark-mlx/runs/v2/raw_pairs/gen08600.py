from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 5) | (F.col("quantity") == 154))
    .select("last_restock", "product_id", "quantity")
)
