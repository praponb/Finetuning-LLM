from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id") == 35)
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
    )
    .orderBy(F.col("session_id").desc())
)
