from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("category") == "Sports")

products_stats = (
    valid_products
    .groupBy("category")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)

result = (
    products_stats
    .filter(F.col("avg_stock_qty") < 40)
    .select("category", "avg_stock_qty")
    .orderBy(F.col("avg_stock_qty").desc(), "category")
    .limit(5)
)
