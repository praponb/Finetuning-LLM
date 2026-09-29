from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_web_events
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w2))
)

result = metrics.select("id", "page", "event_seq", "last_known_page")
