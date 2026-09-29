from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]))
    .select("id", "quantity", "product_id", "store_id")
    .orderBy(F.col("store_id").desc())
)
