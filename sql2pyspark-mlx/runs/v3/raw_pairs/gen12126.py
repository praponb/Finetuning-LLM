from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 116)
    .select("id", "category", F.explode("tags").alias("tag"))
)
