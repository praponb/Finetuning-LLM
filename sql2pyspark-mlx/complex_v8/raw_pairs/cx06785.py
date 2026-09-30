from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_priority_by_event = Window.partitionBy("ticket_id", "priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_agent_id = Window.partitionBy("ticket_id", "agent_id")
w_ticket_id_priority_by_event_2 = Window.partitionBy("ticket_id", "priority").orderBy("event_at", "id")

joined_ranges = (
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

sequenced = (
    joined_ranges
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.min("hours_since_open").over(w_ticket_id_priority_by_event).alias("min_hours_since_open_so_far"),
        F.count(F.lit(1)).over(w_ticket_id_agent_id).alias("rows_in_ticket_id_agent_id"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_priority_by_event_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

result = (
    sequenced
    .groupBy("priority", "event_at")
    .pivot("event_type", ["created", "resolved", "replied"])
    .agg(F.count("min_hours_since_open_so_far").alias("count_v"), F.max("min_hours_since_open_so_far").alias("max_v"))
)
