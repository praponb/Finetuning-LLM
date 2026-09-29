from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "event_time",
        F.rank().over(w).alias("rnk"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)
