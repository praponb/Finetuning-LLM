from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-11-01")
    .select(
        "id",
        "event_type",
        "session_id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
    )
)
