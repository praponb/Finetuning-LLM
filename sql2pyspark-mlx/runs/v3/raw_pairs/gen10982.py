from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .select("id", "event_type", "event_time", F.row_number().over(w).alias("row_num"))
)
