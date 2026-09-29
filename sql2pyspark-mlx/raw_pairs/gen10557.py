from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Sports")
    .groupBy("supplier_id")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)
