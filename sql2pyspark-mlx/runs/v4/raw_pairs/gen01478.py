from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(332, 337))
    .select("id", "brand", F.explode("tags").alias("tag_name"))
)
