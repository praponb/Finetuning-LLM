from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("event_time"), "session_id").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "session_id",
        "duration_sec",
        "page",
        "event_type",
        F.rank().over(w).alias("rnk"),
        F.last("event_type").over(w2).alias("last_event_type"),
    )
    .orderBy("last_event_type", "page")
)
