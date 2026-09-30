from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc(), "id")

latest = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "event_type", "page", "event_time", "duration_sec")
)

result = (
    latest
    .select("id", "page", "event_time", "duration_sec")
    .orderBy(F.col("event_time").desc(), F.col("id").desc())
)
