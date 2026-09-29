from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "event_type",
        "duration_sec",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
    )
)
