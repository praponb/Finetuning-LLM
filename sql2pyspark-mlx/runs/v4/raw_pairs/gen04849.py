from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 2).over(w).alias("next_duration_sec"),
    )
    .orderBy(F.col("next_duration_sec").desc())
)
