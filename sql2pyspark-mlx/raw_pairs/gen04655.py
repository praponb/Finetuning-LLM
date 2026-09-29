from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-11-01")
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy("total_duration_sec")
)
