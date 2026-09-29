from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("session_id") == 23)
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.sum("duration_sec").over(w).alias("session_id_sum_duration_sec"),
        F.rank().over(w2).alias("rnk"),
    )
)
