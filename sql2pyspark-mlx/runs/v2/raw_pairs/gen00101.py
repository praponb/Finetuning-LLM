from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("brand")

result = (
    df_products
    .filter(F.col("brand") == "Stark")
    .groupBy("brand")
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_price", F.sum("total_price").over(w))
    .orderBy("brand", "cumulative_price")
)
