from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-07-01")
    .select("id", "product_id", "store_id", "quantity")
)
