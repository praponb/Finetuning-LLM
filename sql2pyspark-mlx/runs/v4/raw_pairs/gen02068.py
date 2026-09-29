from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w))
    .withColumn("duration_sec_bucket", F.ntile(10).over(w2))
)
