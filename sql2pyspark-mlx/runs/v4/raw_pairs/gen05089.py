from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "*",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
    )
)
