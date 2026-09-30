from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_quarter")

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_price", F.sum("total_price").over(w))
)
