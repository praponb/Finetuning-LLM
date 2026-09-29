from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 7, 8, 9]) & (F.col("product_id").isin([5, 8]) | F.col("product_id").isin([1, 2])))
    .select("id", "store_id", "quantity")
    .orderBy("id")
)
