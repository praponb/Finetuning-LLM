from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([19, 37]))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("duration_sec_rank", F.dense_rank().over(w))
    .orderBy("event_month")
    .limit(5)
)
