from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select("id", "page", "event_time", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("id").desc())
)
