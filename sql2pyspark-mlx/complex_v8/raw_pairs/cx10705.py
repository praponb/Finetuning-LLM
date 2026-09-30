from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

with_children_events = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.prev_event_at",
        "s.session_no",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

sequenced = (
    with_children_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "session_no",
        "event_id",
        "event_event_at",
        "event_event_type",
        F.count(F.lit(1)).over(w_ticket_id_actor).alias("rows_in_ticket_id_actor"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "session_no",
        "event_id",
        "event_event_type",
    )
    .orderBy(F.col("id").desc(), F.col("event_event_type").asc())
)
