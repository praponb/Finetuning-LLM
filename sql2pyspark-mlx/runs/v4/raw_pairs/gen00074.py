from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_30d"),
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_90d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "cumulative_duration_sec",
        "duration_sec_sum_30d",
        "first_known_page",
    )
    .orderBy("page")
)
