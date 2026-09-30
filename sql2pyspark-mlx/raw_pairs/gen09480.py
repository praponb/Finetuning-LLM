from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time")).orderBy("event_time")

result = (
    df_web_events
    .select("id", "event_time", F.dense_rank().over(w).alias("event_time_dense_rank"))
)
