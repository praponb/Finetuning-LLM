from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 385)
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_4"),
        F.lead("duration_sec", 1, 0).over(w2).alias("next_duration_sec"),
    )
    .orderBy(F.col("next_duration_sec").desc())
)
