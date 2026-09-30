from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select("id", "session_id", "duration_sec", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "session_id", "duration_sec")
    .orderBy("duration_sec")
)
