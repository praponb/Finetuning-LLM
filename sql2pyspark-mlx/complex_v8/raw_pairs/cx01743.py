from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

result = (
    sessionized
    .filter(F.col("new_session_flag").between(0, 1))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_ref",
        "prev_event_at",
        "new_session_flag",
        "session_id",
        (F.col("prev_event_at") + F.expr("INTERVAL 15 MINUTES")).alias("prev_event_at_plus_15m"),
        F.date_format(F.col("prev_event_at"), "yyyy-MM-dd HH:00").alias("prev_event_at_label"),
    )
)
