from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

result = (
    df_web_events
    .withColumn("min_duration_sec_by_event_type", F.min("duration_sec").over(w))
    .filter(F.col("duration_sec") == F.col("min_duration_sec_by_event_type"))
    .select("id", "event_type", "duration_sec")
)
