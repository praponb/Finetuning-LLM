from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([10, 11]))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("duration_sec_rank", F.rank().over(w))
    .orderBy(F.col("event_month").desc())
)
