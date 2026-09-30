from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page")

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 24, 30, 34]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.count("*").over(w2).alias("page_count"),
    )
)
