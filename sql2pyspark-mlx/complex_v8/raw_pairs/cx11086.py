from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_hours_since_open = Window.partitionBy("agent_id").orderBy(F.col("hours_since_open").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_agent_id_by_hours_since_open_2 = Window.partitionBy("agent_id").orderBy(F.col("hours_since_open").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

in_window = (
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

screened = (
    in_window.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "email") & (F.col("t.csat") < 4), "left_anti")
    .filter(F.col("event_type") == "assigned")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    screened.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

sequenced = (
    with_stats
    .select(
        "*",
        F.last("ticket_count", ignorenulls=True).over(w_agent_id_by_hours_since_open).alias("last_known_ticket_count"),
        F.round(F.col("hours_since_open") / F.nullif(F.sum("hours_since_open").over(w_agent_id_priority), F.lit(0)), 4).alias("hours_since_open_share_of_agent_id_priority"),
        F.first("event_at").over(w_agent_id_by_hours_since_open_2).alias("first_event_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "last_known_ticket_count",
        "hours_since_open_share_of_agent_id_priority",
        F.greatest(F.col("ticket_count"), F.col("last_known_ticket_count")).alias("greatest_ticket_count_last_known_ticket_count"),
    )
    .orderBy(F.col("greatest_ticket_count_last_known_ticket_count").asc(), "id")
    .limit(20)
)
