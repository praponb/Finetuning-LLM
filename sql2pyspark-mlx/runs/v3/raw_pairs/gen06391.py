from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_price").desc())

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.sum("price").alias("total_price"))
    .withColumn("price_rank", F.rank().over(w))
    .orderBy(F.col("total_price").desc())
)
