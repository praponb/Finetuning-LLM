from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_price").desc())

result = (
    df_products
    .groupBy("brand")
    .agg(F.sum("price").alias("total_price"))
    .withColumn("price_rank", F.dense_rank().over(w))
)
