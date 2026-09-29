from pyspark.sql import functions as F

result = (
    df_inventory
    .select("product_id", "store_id")
    .orderBy("store_id", F.col("product_id").desc())
)
