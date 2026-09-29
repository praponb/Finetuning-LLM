from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "session_id", "event_time", "event_type")
    .orderBy(F.col("id").desc())
)
