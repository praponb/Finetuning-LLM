from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w_grp = Window.partitionBy(F.to_date("event_time"), "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "*",
        F.stddev_samp("duration_sec").over(w_running).alias("session_id_stddev_samp_duration_sec"),
        F.row_number().over(w2).alias("event_seq"),
        F.lag("duration_sec", 2).over(w3).alias("prev_duration_sec"),
        F.sum("duration_sec").over(w_grp).alias("cumulative_duration_sec"),
    )
)
