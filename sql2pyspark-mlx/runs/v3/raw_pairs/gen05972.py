from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("category")

result = (
    df_products
    .filter(F.year("launch_date") >= 2024)
    .groupBy("category")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
    .orderBy(F.col("cumulative_stock_qty").desc(), F.col("category").desc())
)
