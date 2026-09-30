from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_year")

result = (
    df_products
    .filter(F.col("launch_date") > "2024-07-01")
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.sum("price").alias("total_price"))
    .withColumn("cumulative_price", F.sum("total_price").over(w))
)
