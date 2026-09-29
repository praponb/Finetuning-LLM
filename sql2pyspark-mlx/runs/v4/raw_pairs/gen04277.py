from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")

result = (
    df_web_events
    .select("id", "page", "event_time", F.dense_rank().over(w).alias("dense_rnk"))
)
