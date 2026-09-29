from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
)
