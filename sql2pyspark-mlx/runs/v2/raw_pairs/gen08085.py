from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 390) | F.col("stock_qty").between(103, 267))
    .withColumn("margin", F.round((F.col("cost") / F.col("price")), 2))
)
