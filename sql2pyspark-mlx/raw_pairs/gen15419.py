from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
