from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")
w2 = Window.partitionBy(F.to_date("event_time"), "page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        "duration_sec",
        F.rank().over(w).alias("event_time_rank"),
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
    )
)
