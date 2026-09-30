from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(112, 209))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "category")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)
