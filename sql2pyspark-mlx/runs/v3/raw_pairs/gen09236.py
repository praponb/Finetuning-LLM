from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_7"),
    )
    .orderBy("duration_sec", F.col("event_time").desc())
)
