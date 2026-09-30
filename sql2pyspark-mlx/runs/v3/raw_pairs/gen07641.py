from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec")

result = (
    df_web_events
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "duration_sec")
    .orderBy(F.col("duration_sec").desc(), "event_type")
)
