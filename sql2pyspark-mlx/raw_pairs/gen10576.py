from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 5, 11]))
    .select("id", "last_restock", "product_id")
)
