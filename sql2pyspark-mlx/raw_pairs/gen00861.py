from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 1)
)
