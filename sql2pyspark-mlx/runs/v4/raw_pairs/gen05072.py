from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
    .select("id", "page", "duration_sec", "session_id")
    .orderBy(F.col("duration_sec").desc())
)
