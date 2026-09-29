from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_order_items
    .filter(F.col("product_id").isin([11, 16, 21, 27]))
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk", "order_id")
)
