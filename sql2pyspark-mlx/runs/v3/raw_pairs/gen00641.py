from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.first("event_time").over(w).alias("first_event_time"),
    )
)
