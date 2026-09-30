from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1, 0).over(w).alias("prev_duration_sec"),
    )
    .orderBy(F.col("event_type").desc(), F.col("duration_sec").desc())
)
