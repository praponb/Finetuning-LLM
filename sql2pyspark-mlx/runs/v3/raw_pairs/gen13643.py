from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_price").desc())

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk")
)
