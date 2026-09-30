from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_event_at = Window.orderBy(F.col("event_event_at").desc_nulls_first(), "id", "event_id")
w_all_by_event_event_at_full = Window.orderBy("event_event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "assigned"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.channel").isin("chat", "web")) & (F.col("t.requester_id").between(3, 24)), "left_semi")
)

line_level_events = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

metrics = (
    line_level_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_group",
        "event_id",
        "event_event_at",
        F.round((F.unix_timestamp("event_event_at") - F.unix_timestamp(F.lag("event_event_at").over(w_all_by_event_event_at))) / 60, 1).alias("minutes_since_prev_event_event_at"),
        F.nth_value("event_event_at", 2).over(w_all_by_event_event_at_full).alias("second_event_event_at"),
    )
)

result = (
    metrics
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "event_type",
        "event_type_group",
        "event_id",
        "event_event_at",
        "minutes_since_prev_event_event_at",
        "second_event_event_at",
    )
    .orderBy(F.col("id").desc())
)
