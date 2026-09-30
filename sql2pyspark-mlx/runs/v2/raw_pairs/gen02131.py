from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("duration_sec") != 251)
    .select("id", "page", "duration_sec", "event_type", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy(F.col("rn").desc())
)
