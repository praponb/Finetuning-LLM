from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
    )
)
