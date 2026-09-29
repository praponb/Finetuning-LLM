from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

deduped = (
    df_web_events
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select("id", "event_type", "page", "event_time", "duration_sec", "rank_in_group")
)
