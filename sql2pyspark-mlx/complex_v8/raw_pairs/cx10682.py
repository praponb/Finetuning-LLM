from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

with_details_ticket = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 5), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_month",
        "s.prev_event_at",
        "s.new_session_flag",
        "s.session_id",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

enriched_ticket = (
    with_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_month",
        "s.prev_event_at",
        "s.new_session_flag",
        "s.session_id",
        "s.ticket_opened_at",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_month",
        "new_session_flag",
        "session_id",
        F.concat(F.col("actor"), F.lit("#"), F.col("new_session_flag").cast("string")).alias("actor_ref"),
        F.least(F.col("new_session_flag"), F.col("ticket_requester_id")).alias("least_new_session_flag_ticket_requester_id"),
    )
    .orderBy(F.col("new_session_flag").asc(), F.col("event_at").asc_nulls_last(), "id")
    .limit(20)
)
