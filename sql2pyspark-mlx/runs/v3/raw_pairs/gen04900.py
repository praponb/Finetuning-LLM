from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_month")

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
    .orderBy(F.col("cumulative_stock_qty").desc())
)
