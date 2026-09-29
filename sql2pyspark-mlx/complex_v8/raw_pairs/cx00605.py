from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_priority_by_event = Window.partitionBy("event_type", "priority").orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_agent_id_by_hours_since_open = Window.partitionBy("agent_id").orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_agent_id_by_hours_since_open_2 = Window.partitionBy("agent_id").orderBy("hours_since_open", "id")

matched = (
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

with_activity = (
    matched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-18").cast("date")) & (F.col("t.event_type").isin("resolved", "created")), "left_anti")
)

metrics = (
    with_activity
    .select(
        "*",
        F.max("hours_since_open").over(w_event_type_priority_by_event).alias("hours_since_open_max_3"),
        F.sum("hours_since_open").over(w_agent_id_by_hours_since_open).alias("running_hours_since_open"),
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_ticket_id_event_type), 2).alias("hours_since_open_vs_ticket_id_event_type_avg"),
        F.max("hours_since_open").over(w_ticket_id_event_type).alias("ticket_id_event_type_max_hours_since_open"),
        F.ntile(4).over(w_agent_id_by_hours_since_open_2).alias("hours_since_open_quartile"),
    )
)

with_activity_2 = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("resolved", "created")), "left_semi")
)

result = (
    with_activity_2
    .filter(~F.col("event_type").isin("resolved", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "hours_since_open",
        "running_hours_since_open",
        "hours_since_open_vs_ticket_id_event_type_avg",
        "ticket_id_event_type_max_hours_since_open",
        "hours_since_open_quartile",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
    .orderBy("event_at", "ticket_id")
)
