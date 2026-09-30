from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 7, 26, 38]))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc(), F.col("session_id").desc())
)
