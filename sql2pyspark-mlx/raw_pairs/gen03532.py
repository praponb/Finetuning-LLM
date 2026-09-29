from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "session_id",
        "duration_sec",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
    )
)
