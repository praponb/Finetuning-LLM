from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(((F.col("stock_qty") < 106) & F.col("cost").isNotNull()) | (F.col("supplier_id") == 23))
)

products_stats = (
    recent_products
    .groupBy("brand", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = products_stats.select("brand", "launch_month", "max_stock_qty")
