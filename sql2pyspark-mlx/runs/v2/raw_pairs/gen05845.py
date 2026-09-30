from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_unit_price").desc())

result = (
    df_order_items
    .filter(F.col("product_id") == 29)
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("order_id").desc(), "total_unit_price")
    .limit(20)
)
