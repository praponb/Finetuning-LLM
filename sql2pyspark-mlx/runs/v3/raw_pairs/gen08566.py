from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9]) & F.col("product_id").isin([5, 6, 8, 9]) & (F.col("quantity") == 189))
    .select("product_id", "quantity", "last_restock")
    .orderBy("quantity")
)
