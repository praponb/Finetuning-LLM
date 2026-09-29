from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")

result = (
    df_web_events
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("duration_sec").desc())
)
