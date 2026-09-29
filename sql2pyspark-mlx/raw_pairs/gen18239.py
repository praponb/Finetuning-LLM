from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(201, 347))
    .select("id", "product_name", "category", F.explode("tags").alias("tag"))
)
