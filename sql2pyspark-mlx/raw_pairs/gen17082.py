from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("stock_qty") < 153)

summary = (
    filtered_products
    .groupBy("brand")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = summary.filter(F.col("min_stock_qty") < 320)
