from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 420)
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .filter(F.col("total_stock_qty") > 1300)
    .select("launch_month", "stock_qty_count", "total_stock_qty")
    .orderBy("stock_qty_count", "launch_month")
    .limit(5)
)
