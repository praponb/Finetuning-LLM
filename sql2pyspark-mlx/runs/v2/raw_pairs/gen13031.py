from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy(F.col("unit_price").desc(), F.col("order_id").desc())
)
