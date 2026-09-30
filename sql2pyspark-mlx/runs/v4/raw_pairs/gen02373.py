from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("running_duration_sec_2", F.sum("duration_sec").over(w2))
)
