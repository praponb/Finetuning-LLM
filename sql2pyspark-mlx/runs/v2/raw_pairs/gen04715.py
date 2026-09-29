from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy("store_id")
)
