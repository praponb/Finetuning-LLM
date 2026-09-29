from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_month")

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
)
