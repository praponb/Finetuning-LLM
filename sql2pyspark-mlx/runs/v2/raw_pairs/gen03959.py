from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        "session_id",
        "event_time",
        F.first("session_id").over(w).alias("first_session_id"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_4"),
    )
)
