from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/home"]))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1, 0).over(w).alias("prev_duration_sec"),
    )
)
