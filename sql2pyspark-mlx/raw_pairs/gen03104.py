from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page").isNull())
    .select("id", "event_type", "event_time", F.ntile(2).over(w).alias("quartile"))
    .orderBy("event_type")
)
