from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-04-01")
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
    .orderBy("row_num")
)
