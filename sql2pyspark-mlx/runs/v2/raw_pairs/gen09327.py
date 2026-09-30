from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .select("id", "product_name", F.explode("tags").alias("label"))
)
