from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_ticket_id = Window.partitionBy("priority", "ticket_id")
w_all_by_hours_since_open = Window.orderBy(F.col("hours_since_open").desc_nulls_first(), "id")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

metrics = (
    in_window
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.round(F.col("hours_since_open") / F.nullif(F.sum("hours_since_open").over(w_priority_ticket_id), F.lit(0)), 4).alias("hours_since_open_share_of_priority_ticket_id"),
        F.lag("hours_since_open").over(w_all_by_hours_since_open).alias("prev_hours_since_open"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "hours_since_open_share_of_priority_ticket_id",
        "prev_hours_since_open",
        (F.col("prev_hours_since_open") - F.col("hours_since_open")).alias("prev_hours_since_open_minus_hours_since_open"),
    )
    .orderBy(F.col("prev_hours_since_open_minus_hours_since_open").desc())
)
