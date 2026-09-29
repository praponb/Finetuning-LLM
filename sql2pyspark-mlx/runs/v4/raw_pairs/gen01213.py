from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
    .filter(F.col("max_stock_qty") >= 210)
    .select("category", "max_stock_qty")
    .orderBy("category")
)
