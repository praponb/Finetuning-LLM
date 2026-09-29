from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2023-05-01")
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("revenue").desc())
)
