from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 7, 9]))
    .select("quantity", "product_id", "last_restock")
)
