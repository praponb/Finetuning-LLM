from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_hours_since_open = Window.partitionBy("priority").orderBy(F.col("hours_since_open").desc_nulls_first(), "id")
w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_agent_id_by_event = Window.partitionBy("ticket_id", "agent_id").orderBy("event_at")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

ranked = (
    in_window
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_priority_by_hours_since_open)).alias("hours_since_open_change"),
        F.max("hours_since_open").over(w_event_type_ticket_id).alias("event_type_ticket_id_max_hours_since_open"),
        F.sum("hours_since_open").over(w_all_by_event_at_running).alias("running_hours_since_open"),
        F.dense_rank().over(w_ticket_id_agent_id_by_event).alias("event_at_dense_rank"),
        F.sum("hours_since_open").over(w_event_type_ticket_id).alias("event_type_ticket_id_sum_hours_since_open"),
    )
)

top_per_group = ranked.filter(F.col("event_at_dense_rank") <= 5)

result = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_ticket_id_max_hours_since_open",
        "running_hours_since_open",
        "event_at_dense_rank",
        F.month(F.col("event_at")).alias("event_at_month"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
    .orderBy(
        "event_type_ticket_id_max_hours_since_open",
        F.col("ticket_id").asc_nulls_last(),
    )
)
