from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 185)
    .select("id", "category", "product_name", F.explode("tags").alias("label"))
)
