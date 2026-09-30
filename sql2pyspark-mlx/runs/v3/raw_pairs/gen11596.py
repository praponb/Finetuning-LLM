from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]) & F.month("last_restock").isin([2, 4, 5]))
    .select("id", "quantity", "product_id", "store_id")
)
