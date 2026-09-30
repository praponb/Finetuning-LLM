from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
    .orderBy("category")
)
