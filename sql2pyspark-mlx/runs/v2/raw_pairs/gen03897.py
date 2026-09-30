from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("brand")

result = (
    df_products
    .filter(F.col("brand") != "Initech")
    .groupBy("brand")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
    .orderBy(F.col("cumulative_stock_qty").desc(), "brand")
    .limit(10)
)
