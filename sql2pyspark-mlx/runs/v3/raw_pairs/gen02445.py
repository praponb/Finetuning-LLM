from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("brand")

result = (
    df_products
    .groupBy("brand")
    .agg(F.sum("price").alias("total_price"))
    .withColumn("cumulative_price", F.sum("total_price").over(w))
    .orderBy(F.col("brand").desc())
)
