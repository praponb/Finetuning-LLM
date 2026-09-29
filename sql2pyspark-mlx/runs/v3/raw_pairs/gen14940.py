from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(180, 349))
    .select("id", "product_name", "brand", F.explode("tags").alias("tag_name"))
)
