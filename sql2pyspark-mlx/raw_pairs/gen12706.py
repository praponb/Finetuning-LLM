from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 7, 9]) & ((F.col("quantity") < 169) | F.col("product_id").isin([6, 9, 10])))
    .select("id", "last_restock", "store_id", "product_id")
)
