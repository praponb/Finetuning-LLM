from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 270) & (F.col("category") == "Grocery"))
    .groupBy("category")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
    .filter(F.col("avg_stock_qty") < 52)
    .orderBy(F.col("avg_stock_qty").desc())
)
