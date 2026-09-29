from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select("id", "session_id", "page", "event_time", "duration_sec", "dedup_rank")
)
