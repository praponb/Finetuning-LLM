from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_4"),
        F.rank().over(w2).alias("duration_sec_rank"),
    )
)
