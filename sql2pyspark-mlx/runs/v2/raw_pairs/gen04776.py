from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 320)
    .select("id", "product_name", "category", F.explode("tags").alias("label"))
)
