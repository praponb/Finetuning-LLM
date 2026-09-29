from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 109)
    .select("id", "price", F.explode("tags").alias("tag"))
)
