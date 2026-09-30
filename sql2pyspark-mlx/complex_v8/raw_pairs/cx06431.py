from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_event_type_running))

tagged = (
    sessionized
    .filter(F.col("prev_event_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.round(F.col("session_id") / F.nullif(F.col("is_new_session"), F.lit(0)), 3).alias("session_id_per_is_new_session"),
    )
)

with_details_ticket = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details_ticket
    .filter(F.to_date(F.col("prev_event_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "prev_event_at",
        "is_new_session",
        "actor_ref",
        "event_at_quarter",
        "session_id_per_is_new_session",
        "ticket_requester_id",
        "ticket_opened_at",
    )
    .orderBy(F.col("event_at").asc())
)
