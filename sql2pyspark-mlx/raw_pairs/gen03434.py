from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "is_new_session",
        "island_id",
    )
    .orderBy("id")
)
