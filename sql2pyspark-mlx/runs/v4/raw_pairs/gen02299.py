from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") < 168) | (F.col("product_id") == 9))
    .select("last_restock", "store_id", "quantity")
)
