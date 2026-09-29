from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("cost").between(310, 620))

products_stats = (
    recent_products
    .groupBy("category")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = (
    products_stats
    .filter(F.col("max_stock_qty") < 220)
    .select("category", "max_stock_qty")
    .orderBy(F.col("category").desc())
    .limit(20)
)
