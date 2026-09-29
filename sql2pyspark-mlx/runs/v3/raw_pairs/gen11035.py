from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "page",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
    )
)
