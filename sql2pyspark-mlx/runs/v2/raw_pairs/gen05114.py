from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2025-10-01") | F.col("product_id").isin([1, 3, 5, 8]))
    .select("id", "product_id", "last_restock")
)
