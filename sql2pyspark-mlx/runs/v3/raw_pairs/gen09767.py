from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.max("launch_date").alias("last_launch_date"),
        F.count("price").alias("price_count"),
    )
    .orderBy("last_launch_date", "launch_year")
    .limit(10)
)
