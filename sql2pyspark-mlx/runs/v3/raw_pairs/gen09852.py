from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("duration_sec").between(211, 408))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_duration_sec").desc(), "event_month")
    .limit(50)
)
