from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
)
