from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc())
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first())

result = (
    df_web_events
    .withColumn("duration_sec_bucket", F.ntile(3).over(w))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w2))
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w3))
)
