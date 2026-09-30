from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.col("store_id").isin([3, 6]) | F.col("product_id").isin([2, 5, 7])))
    .select("quantity", "store_id")
    .orderBy("store_id")
)
