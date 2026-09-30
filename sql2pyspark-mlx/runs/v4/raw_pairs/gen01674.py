from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost").between(610, 720) & (F.col("cost") < 280)) | (F.col("category") == "Home"))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
    .orderBy(F.col("avg_stock_qty").desc())
)
