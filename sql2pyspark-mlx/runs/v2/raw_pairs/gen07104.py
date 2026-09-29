from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("session_id").isin([16, 26, 27, 30]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_3"),
    )
)
