from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type")

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "event_type",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.avg("duration_sec").over(w2).alias("event_type_avg_duration_sec"),
    )
    .orderBy(F.col("event_type_avg_duration_sec").desc())
)
