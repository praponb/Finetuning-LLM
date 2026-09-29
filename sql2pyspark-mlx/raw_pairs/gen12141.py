from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w_hist))
    .withColumn("prev_duration_sec", F.lag("duration_sec", 2).over(w3))
)
