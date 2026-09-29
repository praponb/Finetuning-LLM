from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .filter(F.col("page") != "/search")
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "page",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.last("duration_sec").over(w2).alias("last_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc())
)
