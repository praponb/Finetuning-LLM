from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 210) & F.col("product_id").isin([1, 9, 10]))
    .select("store_id")
)
