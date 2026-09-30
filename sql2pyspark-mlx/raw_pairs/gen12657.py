from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("event_time"), "page").orderBy("event_time", "id")
w2 = Window.partitionBy("page")

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 13, 23, 33]))
    .select(
        "id",
        "event_time",
        "page",
        "duration_sec",
        F.lead("duration_sec", 1, 0).over(w).alias("next_duration_sec"),
        F.min("duration_sec").over(w2).alias("page_min_duration_sec"),
    )
)
