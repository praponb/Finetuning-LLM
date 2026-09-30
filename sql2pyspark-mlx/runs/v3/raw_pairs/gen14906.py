from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 6, 12]) & ((F.col("last_restock") <= "2025-01-01") | F.col("product_id").isin([8, 9, 10])))
    .select("id", "product_id", "store_id", "quantity")
)
