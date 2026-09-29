from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("duration_sec", "id")

enriched = (
    df_web_events
    .select(
        "id",
        "event_time",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
        F.ntile(10).over(w2).alias("duration_sec_bucket"),
    )
)

result = (
    enriched
    .select("id", "event_time", "prev_duration_sec")
    .orderBy(F.col("event_time").desc())
)
