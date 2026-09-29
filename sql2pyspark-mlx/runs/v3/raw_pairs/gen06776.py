from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("price").between(510, 960))
    .withColumn("adjusted_price", F.round((F.col("price") * 0.85), 2))
)

agg = (
    base
    .groupBy("category")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    agg
    .orderBy("avg_stock_qty", "category")
    .limit(10)
)
