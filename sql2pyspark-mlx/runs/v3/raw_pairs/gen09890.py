from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-11-01")
    .select("id", "event_type", "duration_sec", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
)
