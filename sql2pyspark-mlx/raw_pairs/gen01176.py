from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy(F.month("event_time"), "page")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "page",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.avg("duration_sec").over(w2).alias("event_time_month_avg_duration_sec"),
    )
)
