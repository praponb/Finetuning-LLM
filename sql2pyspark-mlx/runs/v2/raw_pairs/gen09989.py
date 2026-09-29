from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "event_type", "duration_sec", "session_id")
    .orderBy(F.col("id").desc())
)
