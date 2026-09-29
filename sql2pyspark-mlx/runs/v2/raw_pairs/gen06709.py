from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec").between(406, 459))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec", 1).over(w2)).alias("duration_sec_change"),
    )
)
