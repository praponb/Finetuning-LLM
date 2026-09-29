from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(171, 183))
    .select("id", "price", F.explode("tags").alias("tag"))
)
