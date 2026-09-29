from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select("id", "page", "event_time", "event_type", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "page", "event_time", "event_type")
    .orderBy(F.col("event_type").desc())
)
