from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") < 367)
    .select("id", "event_type", "event_time", F.row_number().over(w).alias("rn"))
    .orderBy("rn", F.col("id").desc())
)
