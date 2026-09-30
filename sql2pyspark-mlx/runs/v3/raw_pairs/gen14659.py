from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select("id", "event_type", "event_time", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "event_type", "event_time")
    .orderBy(F.col("event_time").desc(), "id")
)
