from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "session_id",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.avg("duration_sec").over(w2).alias("session_id_avg_duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc(), F.col("event_time").desc())
)
