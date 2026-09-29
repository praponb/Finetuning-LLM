from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .select("id", "product_id")
)
