from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select("id", "event_type", "duration_sec", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("duration_sec").desc())
)
