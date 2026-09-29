from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2023) & F.col("store_id").isin([2, 4, 5]))
    .select("id", "quantity", "last_restock", "product_id")
)
