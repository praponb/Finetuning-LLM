from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .select("id", "product_id", "quantity", "store_id")
    .orderBy(F.col("store_id").desc())
)
