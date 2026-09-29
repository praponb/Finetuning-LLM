from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.dense_rank().over(w))
)
