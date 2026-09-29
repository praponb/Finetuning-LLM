from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-03-01")
    .select("id", "session_id", "event_time", F.dense_rank().over(w).alias("dense_rnk"))
)
