from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("event_time"), "session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w_part).alias("running_avg_duration_sec"),
        F.sum("duration_sec").over(w_part).alias("running_sum_duration_sec"),
        F.max("duration_sec").over(w_part).alias("running_max_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
    )
)
