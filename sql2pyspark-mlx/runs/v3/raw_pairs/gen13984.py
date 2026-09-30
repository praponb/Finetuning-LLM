from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 360)
    .select("id", "brand", "category", F.explode("tags").alias("label"))
)
