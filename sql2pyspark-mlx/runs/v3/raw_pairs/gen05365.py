from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 5, 6]))
    .select("store_id")
    .intersect(df_inventory.filter(F.col("product_id") == 10).select("store_id"))
)
