from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .select("id", "event_type", "duration_sec", "session_start_flag", "session_no")
    .orderBy(F.col("event_type").desc_nulls_first())
)
