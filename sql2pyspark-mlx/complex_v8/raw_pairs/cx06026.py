from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_ticket = (
    sessions.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.csat") < 4)
    .select(
        "s.ticket_id",
        "s.burst_id",
        "s.events",
        "s.duration_min",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_parent_ticket
    .select(
        "ticket_id",
        "burst_id",
        "events",
        "ticket_tags",
        F.abs(F.col("burst_id") - 4).alias("burst_id_distance"),
    )
    .orderBy(F.col("ticket_id").desc(), F.col("events").desc())
)
