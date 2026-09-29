from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("cost").between(340, 400))
    .withColumn("launch_date_month", F.month("launch_date"))
)

products_stats = (
    valid_products
    .groupBy("category")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    products_stats
    .select("category", "avg_stock_qty")
    .orderBy("avg_stock_qty")
)
