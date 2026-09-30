from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_type")

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(F.col("total_duration_sec").desc(), "event_type")
)
