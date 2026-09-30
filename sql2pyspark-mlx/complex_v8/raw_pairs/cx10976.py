from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_priority_by_hours_since_open = Window.partitionBy("priority").orderBy(F.col("hours_since_open").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_hours_since_open_2 = Window.partitionBy("priority").orderBy(F.col("hours_since_open").desc(), "id")

joined_ranges = (
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
    joined_ranges
    .select(
        "*",
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_priority), 2).alias("hours_since_open_vs_priority_avg"),
        F.sum("hours_since_open").over(w_priority_by_hours_since_open).alias("running_hours_since_open"),
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_priority_by_hours_since_open_2)).alias("hours_since_open_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_type",
        "agent_id",
        "hours_since_open",
        "hours_since_open_change",
        F.least(F.col("hours_since_open"), F.col("hours_since_open_change")).alias("least_hours_since_open_hours_since_open_change"),
    )
    .orderBy("ticket_id", F.col("event_type").asc(), "id")
    .limit(10)
)
