from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2024-04-17") & F.col("last_restock").between("2023-05-01", "2024-07-01"))
    .select("store_id", "product_id", "last_restock")
)
