from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .orderBy(F.col("session_id").desc(), F.col("event_type").desc())
)
