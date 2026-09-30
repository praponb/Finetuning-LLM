from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "session_id")
w_recent = Window.orderBy("event_time", "id")
w_ordered_2 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("event_time"), "session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.count("duration_sec").over(w_ordered).alias("event_time_period_count_duration_sec"),
        F.lag("duration_sec", 1).over(w_recent).alias("prev_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_ordered_2).alias("last_known_duration_sec"),
        F.ntile(10).over(w4).alias("duration_sec_bucket"),
    )
)
