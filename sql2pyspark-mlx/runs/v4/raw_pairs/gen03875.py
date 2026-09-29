from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_stock_qty").desc())

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
)
