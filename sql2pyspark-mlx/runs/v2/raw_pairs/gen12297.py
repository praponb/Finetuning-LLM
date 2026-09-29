from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_stock_qty").desc())

result = (
    df_products
    .filter(F.col("stock_qty") >= 376)
    .groupBy("brand")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("stock_qty_rank", F.dense_rank().over(w))
    .orderBy("total_stock_qty")
)
