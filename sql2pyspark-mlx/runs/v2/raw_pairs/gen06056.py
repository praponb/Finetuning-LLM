from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("duration_sec").between(263, 422))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "page",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_3"),
    )
)
