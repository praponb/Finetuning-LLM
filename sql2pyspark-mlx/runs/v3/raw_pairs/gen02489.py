from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "page", "duration_sec")
    .orderBy(F.col("duration_sec").desc())
)
