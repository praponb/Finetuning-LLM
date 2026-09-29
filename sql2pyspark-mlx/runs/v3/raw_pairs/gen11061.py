from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "event_type", "duration_sec", "session_id")
)
