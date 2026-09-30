from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy("product_id")
)
