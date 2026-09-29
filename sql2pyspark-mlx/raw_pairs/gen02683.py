from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 7, 10]))
    .select("id", "product_id")
)
