from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_event_at = Window.partitionBy("priority").orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

joined_ticket = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

screened = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

with_windows = (
    screened
    .select(
        "id",
        "event_at",
        "agent_id",
        "ticket_priority",
        F.row_number().over(w_priority_by_event_at).alias("seq_no"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_at",
        "agent_id",
        "ticket_priority",
        "seq_no",
        "second_event_at",
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket_2"),
    )
    .orderBy(F.col("seq_no").desc())
)
