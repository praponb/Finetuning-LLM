from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("page")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "page",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_3"),
        F.count("*").over(w2).alias("page_count"),
    )
    .orderBy(F.col("duration_sec").desc())
)
