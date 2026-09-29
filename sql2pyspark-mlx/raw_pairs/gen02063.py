from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.cume_dist().over(w).alias("duration_sec_cume_dist"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
    )
)
