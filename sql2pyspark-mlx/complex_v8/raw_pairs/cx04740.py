from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_ticket_id = Window.partitionBy("priority", "ticket_id")
w_priority_event_type_by_event = Window.partitionBy("priority", "event_type").orderBy(F.col("event_at").desc(), "id")
w_event_at_seq_by_event_at_seq = Window.partitionBy("event_at_seq").orderBy("event_at_seq", "id")
w_event_at_seq_by_event_at_seq_2 = Window.partitionBy("event_at_seq").orderBy("event_at_seq")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

with_trends = (
    covered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "agent_id",
        "hours_since_open",
        F.sum("hours_since_open").over(w_priority_ticket_id).alias("priority_ticket_id_sum_hours_since_open"),
        F.row_number().over(w_priority_event_type_by_event).alias("event_at_seq"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "agent_id",
        "hours_since_open",
        "priority_ticket_id_sum_hours_since_open",
        "event_at_seq",
        F.lag("event_at", 2).over(w_event_at_seq_by_event_at_seq).alias("prev2_event_at"),
        F.rank().over(w_event_at_seq_by_event_at_seq_2).alias("event_at_seq_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_at",
        "priority",
        "event_at_seq",
        "prev2_event_at",
        "event_at_seq_rank",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
