from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 31)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_ticket_id_running))

session_summary = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

joined_ticket = (
    session_summary.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.ticket_id",
        "s.burst_id",
        "s.session_start",
        "s.session_end",
        "s.events",
        "s.duration_min",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    joined_ticket
    .filter(F.col("duration_min") > 96.0)
    .select(
        "ticket_id",
        "session_start",
        "session_end",
        "duration_min",
        "ticket_priority",
        "ticket_tags",
        F.substring(F.trim(F.col("ticket_priority")), 1, 4).alias("ticket_priority_prefix"),
        F.round(F.col("burst_id") / F.nullif(F.col("events"), F.lit(0)), 3).alias("burst_id_per_events"),
    )
    .orderBy(F.col("burst_id_per_events").desc(), F.col("ticket_priority").desc())
)
