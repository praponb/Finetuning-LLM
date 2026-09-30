from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") == 240)
    .select("id", "price", "brand", F.explode("tags").alias("label"))
)
