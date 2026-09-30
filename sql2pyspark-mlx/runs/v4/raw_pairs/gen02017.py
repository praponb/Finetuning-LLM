from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(150, 289))
    .select("id", "brand", "price", F.explode("tags").alias("tag_name"))
)
