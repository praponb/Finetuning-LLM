from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "cost", "shipped_date")
    .orderBy("shipped_date")
)
