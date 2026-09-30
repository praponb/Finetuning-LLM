from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_or_unknown_event = Window.partitionBy("event_type_or_unknown", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_or_unknown_event_2 = Window.partitionBy("event_type_or_unknown", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_or_unknown_event_3 = Window.partitionBy("event_type_or_unknown", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_or_unknown_event_4 = Window.partitionBy("event_type_or_unknown", "event_type").orderBy("event_at", "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

sequenced = (
    scoped_events
    .select(
        "*",
        F.round((F.unix_timestamp("event_at_plus_1d") - F.unix_timestamp(F.lag("event_at_plus_1d").over(w_event_type_or_unknown_event))) / 60, 1).alias("minutes_since_prev_event_at_plus_1d"),
        F.nth_value("event_at", 2).over(w_event_type_or_unknown_event_2).alias("second_event_at"),
        F.last("event_at").over(w_event_type_or_unknown_event_3).alias("last_event_at"),
        F.lag("event_at", 3).over(w_event_type_or_unknown_event_4).alias("prev3_event_at"),
        F.lead("event_at_plus_1d", 2).over(w_event_type_or_unknown_event).alias("next2_event_at_plus_1d"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_or_unknown",
        "s.actor_slug",
        "s.event_at_plus_1d",
        "s.minutes_since_prev_event_at_plus_1d",
        "s.second_event_at",
        "s.last_event_at",
        "s.prev3_event_at",
        F.col("t.csat").alias("ticket_csat"),
    )
)

filtered = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P1"), "left_semi")
)

combined_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
    )
)

with_activity = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority").isNotNull()) & (F.col("t.csat") <= 4), "left_semi")
)

result = (
    with_activity
    .groupBy("event_type", "last_event_at")
    .pivot("actor", ["customer", "agent", "system"])
    .agg(F.sum("minutes_since_prev_event_at_plus_1d"))
)
