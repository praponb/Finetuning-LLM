from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_year")

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(F.col("cumulative_duration_sec").desc())
)
