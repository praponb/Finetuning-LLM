from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 208)
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
)
