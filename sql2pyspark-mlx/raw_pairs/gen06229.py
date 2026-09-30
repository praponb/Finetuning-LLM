from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec")
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        "page",
        "event_time",
        F.rank().over(w).alias("rnk"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc(), "event_type")
)
