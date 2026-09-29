from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 13]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
    )
)
