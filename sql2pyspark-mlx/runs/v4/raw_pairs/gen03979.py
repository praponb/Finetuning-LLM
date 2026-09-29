from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time", "id")
w2 = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .select(
        "*",
        F.lead("duration_sec", 1, 0).over(w).alias("next_duration_sec"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w3).alias("last_known_duration_sec"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "next_duration_sec",
        "running_duration_sec",
    )
    .orderBy("duration_sec")
)
