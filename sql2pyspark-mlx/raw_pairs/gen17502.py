from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_month")

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.sum("price").alias("total_price"))
    .withColumn("cumulative_price", F.sum("total_price").over(w))
)
