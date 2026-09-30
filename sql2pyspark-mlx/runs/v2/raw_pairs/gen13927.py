from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("unit_price").between(220, 400))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
)
