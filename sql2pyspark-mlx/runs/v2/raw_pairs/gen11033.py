from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "session_id",
        (F.col("duration_sec") - F.lag("duration_sec", 1).over(w)).alias("duration_sec_change"),
        F.sum("duration_sec").over(w2).alias("session_id_sum_duration_sec"),
    )
)
