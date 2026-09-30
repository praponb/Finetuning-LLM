from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 149)
    .withColumn("margin", (F.col("cost") * F.col("price")))
    .orderBy(F.col("product_name").desc())
)
