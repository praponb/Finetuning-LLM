from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_unit_price").desc())

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_unit_price").desc())
)
