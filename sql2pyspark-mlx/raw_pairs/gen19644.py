from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("page", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.max("duration_sec").over(w).alias("last4_max_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
        F.min("duration_sec").over(w).alias("last4_min_duration_sec"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_type",
        "page",
        "last4_max_duration_sec",
        "last4_count_duration_sec",
        "last4_min_duration_sec",
    )
    .orderBy(F.col("last4_count_duration_sec").desc())
)
