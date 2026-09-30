from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select("id", "event_type", "duration_sec", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "event_type", "duration_sec")
)
