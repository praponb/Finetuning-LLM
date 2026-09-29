from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_ticket_id_running))

session_summary = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched_ticket = (
    session_summary.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.closed_at")) < F.lit("2025-02-22").cast("date")), "left")
    .select(
        "s.ticket_id",
        "s.session_id",
        "s.session_end",
        "s.duration_min",
        F.col("t.priority").alias("ticket_priority"),
    )
)

enriched = (
    enriched_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.to_date(F.col("t.closed_at")) < F.lit("2025-02-22").cast("date"))
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    enriched
    .filter(F.col("session_end") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "ticket_id",
        "session_id",
        "session_end",
        "duration_min",
        "ticket_closed_at",
        F.hour(F.col("ticket_closed_at")).alias("ticket_closed_at_hour"),
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
)
