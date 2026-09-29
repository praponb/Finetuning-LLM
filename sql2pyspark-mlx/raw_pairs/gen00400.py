from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-03-17")
    .select("id", "product_id", "quantity", "store_id")
)
