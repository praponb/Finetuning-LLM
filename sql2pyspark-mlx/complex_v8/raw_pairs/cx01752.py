from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group_by_event_at = Window.partitionBy("actor_group").orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_ticket_id_b = Window.partitionBy(F.to_date("event_at"), "ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(2, 3))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_details_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.size(F.col("t.tags")) >= 1)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_actor_label",
        "s.actor_group",
        F.col("t.tags").alias("ticket_tags"),
    )
)

without_matches = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "assigned"), "left_anti")
    .filter(F.array_contains(F.col("ticket_tags"), "outage"))
)

with_trends = (
    without_matches
    .select(
        "*",
        F.ntile(4).over(w_actor_group_by_event_at).alias("event_at_quartile"),
        F.ntile(3).over(w_actor_group_by_event_at).alias("event_at_tercile"),
        F.last("event_at").over(w_event_at_to_date_ticket_id_b).alias("last_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_actor_label",
        "actor_group",
        "ticket_tags",
        "event_at_quartile",
        "event_at_tercile",
        "last_event_at",
    )
    .orderBy(F.col("event_type").asc_nulls_last(), "event_at", "id")
    .limit(20)
)
