from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([6, 37]))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 2).over(w).alias("next_duration_sec"),
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec_2"),
    )
)
