from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

result = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)
