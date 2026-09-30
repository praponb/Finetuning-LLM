from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_day")

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(F.col("cumulative_duration_sec").desc(), "event_day")
    .limit(10)
)
