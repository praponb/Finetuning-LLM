from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .select("id", "session_id", "event_time", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "session_id", "event_time")
)
