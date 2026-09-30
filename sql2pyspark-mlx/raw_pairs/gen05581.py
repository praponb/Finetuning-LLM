from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("event_time")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
    )
)
