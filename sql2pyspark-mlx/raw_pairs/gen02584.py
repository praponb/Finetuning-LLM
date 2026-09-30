from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.min("duration_sec").over(w).alias("last3_min_duration_sec"),
        F.avg("duration_sec").over(w).alias("last3_avg_duration_sec"),
        F.count("duration_sec").over(w).alias("last3_count_duration_sec"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
    )
)
