from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 151) & (F.col("store_id") == 6))
    .select("id", "product_id", F.col("quantity").cast("double").alias("quantity_str"))
)
