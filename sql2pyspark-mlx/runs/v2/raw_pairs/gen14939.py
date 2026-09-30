from pyspark.sql import functions as F

filtered_products = df_products.filter((F.col("cost") < 400) | (F.size("tags") > 2))

summary = (
    filtered_products
    .groupBy("supplier_id", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    summary
    .filter(F.col("avg_stock_qty") > 340)
    .select("supplier_id", "launch_month", "avg_stock_qty")
)
