from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        (F.max("price") - F.min("price")).alias("price_range"),
        F.min("launch_date").alias("min_launch_date"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .filter(F.col("min_stock_qty") > 200)
    .drop("min_stock_qty")
    .orderBy("launch_month")
    .limit(5)
)
