from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 5)
    .select("brand", "avg_stock_qty", "cnt")
)
