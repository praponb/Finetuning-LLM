from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_web_events
    .select("id", "event_type", "event_time", "duration_sec", "burst_id")
    .orderBy(F.col("event_time").desc_nulls_last())
)
