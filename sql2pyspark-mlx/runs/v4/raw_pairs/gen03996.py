from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("duration_sec") < 238)
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "event_type", "event_time", "session_id")
)
