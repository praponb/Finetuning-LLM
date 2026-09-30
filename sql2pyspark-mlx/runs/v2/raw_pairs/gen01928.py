from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 170)
    .select("id", "brand", "category", F.explode("tags").alias("tag_name"))
)
