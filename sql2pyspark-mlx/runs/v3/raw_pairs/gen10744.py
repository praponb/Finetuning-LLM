from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("event_type")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("duration_sec_rank", F.rank().over(w))
    .orderBy(F.col("duration_sec_rank").desc(), "event_type")
    .limit(50)
)
