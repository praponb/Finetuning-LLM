from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-08-13")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "event_type", "duration_sec", "event_time")
)
