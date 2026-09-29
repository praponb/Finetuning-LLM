from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .select("id", "session_id", "event_type", "page", "new_group_flag", "burst_id")
    .orderBy(F.col("event_type").desc(), "burst_id")
)
