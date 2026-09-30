from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("event_type")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "event_type",
        F.last("duration_sec").over(w).alias("last_duration_sec"),
        F.min("duration_sec").over(w2).alias("event_type_min_duration_sec"),
    )
)
