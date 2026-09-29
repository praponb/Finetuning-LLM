from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 6, 12]))
    .select("id", "product_id")
)
