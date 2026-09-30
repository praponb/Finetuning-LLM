from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_day")

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 16))
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(F.col("total_duration_sec").desc(), F.col("event_day").desc())
)
