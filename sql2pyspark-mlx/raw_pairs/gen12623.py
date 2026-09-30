from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 1) & (F.col("store_id") == 1))
    .select("product_id", "quantity")
)
