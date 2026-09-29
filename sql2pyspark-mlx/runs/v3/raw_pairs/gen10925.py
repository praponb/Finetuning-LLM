from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") != 214)
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.last("event_time").over(w).alias("last_event_time"),
        F.lead("duration_sec", 1, 0).over(w2).alias("next_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc())
)
