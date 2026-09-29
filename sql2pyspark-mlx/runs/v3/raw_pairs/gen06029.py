from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
        F.percent_rank().over(w2).alias("duration_sec_pct_rank"),
    )
)
