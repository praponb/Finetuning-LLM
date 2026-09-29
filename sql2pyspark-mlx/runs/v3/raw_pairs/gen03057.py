from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout", "/home"]))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_id",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
