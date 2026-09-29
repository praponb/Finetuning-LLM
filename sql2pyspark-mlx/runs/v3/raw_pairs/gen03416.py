from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        "duration_sec",
        F.row_number().over(w).alias("row_num"),
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
    )
)
