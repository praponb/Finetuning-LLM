from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("category")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
    .filter(F.col("min_stock_qty") >= 220)
    .orderBy(F.col("category").desc())
)
