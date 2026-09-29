from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-08-17")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
)
