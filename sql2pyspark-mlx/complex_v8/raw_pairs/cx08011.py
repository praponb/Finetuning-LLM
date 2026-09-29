from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(9, 12))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = recent.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

result = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "new_session_flag",
        "session_id",
        F.date_trunc("week", F.col("prev_event_at")).alias("prev_event_at_week"),
        F.abs(F.col("new_session_flag") - 0).alias("new_session_flag_distance"),
    )
    .orderBy(F.col("prev_event_at_week").desc())
)
