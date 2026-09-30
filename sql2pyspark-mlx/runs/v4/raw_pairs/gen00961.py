from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("duration_sec") < 284)
    .select(
        "id",
        "page",
        "session_id",
        "event_time",
        "duration_sec",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_7"),
    )
    .orderBy("event_time", F.col("page").desc())
)
