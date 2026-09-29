from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        "duration_sec",
        F.percent_rank().over(w).alias("event_time_pct_rank"),
        F.lag("duration_sec", 2).over(w2).alias("prev_duration_sec"),
    )
    .orderBy("event_time_pct_rank")
)
