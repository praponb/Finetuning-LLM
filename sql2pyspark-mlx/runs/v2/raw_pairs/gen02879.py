from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 10, 11]))
    .select("quantity", "product_id", "store_id")
)
