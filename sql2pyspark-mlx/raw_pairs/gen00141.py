from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "is_new_burst",
        "island_id",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").desc())
)
