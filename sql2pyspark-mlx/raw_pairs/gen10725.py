from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type") != "search")
    .select("id", "session_id", "duration_sec", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "session_id", "duration_sec")
    .orderBy(F.col("id").desc())
)
