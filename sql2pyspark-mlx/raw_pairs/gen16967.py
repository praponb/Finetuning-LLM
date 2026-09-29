from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") != 393)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "page", "event_time")
    .orderBy(F.col("event_time").desc())
)
