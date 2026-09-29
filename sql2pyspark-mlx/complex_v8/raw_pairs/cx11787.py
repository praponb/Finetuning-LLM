from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_ticket_id_by_ticket = Window.partitionBy("event_id", "ticket_id").orderBy("ticket_requester_id_minus_max")
w_event_id_ticket_id_by_ticket_2 = Window.partitionBy("event_id", "ticket_id").orderBy("ticket_requester_id_minus_max", "id", "event_id").rowsBetween(-6, Window.currentRow)
w_event_id_ticket_id_by_ticket_3 = Window.partitionBy("event_id", "ticket_id").orderBy("ticket_requester_id_minus_max", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("ticket_id") >= 29))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

joined_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

line_level = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.ticket_csat",
        "s.agent_team",
        "s.agent_name",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

joined_ticket_2 = (
    line_level.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id") >= 9)
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_team",
        "s.agent_name",
        "s.event_id",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_details = (
    joined_ticket_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_team",
        "s.agent_name",
        "s.event_id",
        "s.ticket_requester_id",
        "s.ticket_closed_at",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

ticket_id_stats = (
    with_details
    .groupBy("ticket_id")
    .agg(F.max("ticket_requester_id").alias("max_ticket_requester_id"))
)

above_average = (
    with_details.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.ticket_requester_id") == F.col("g.max_ticket_requester_id"))
    .select(
        "s.*",
        "g.max_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.max_ticket_requester_id"), 2).alias("ticket_requester_id_minus_max"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.dense_rank().over(w_event_id_ticket_id_by_ticket).alias("ticket_requester_id_minus_max_dense_rank"),
        F.sum("ticket_requester_id").over(w_event_id_ticket_id_by_ticket_2).alias("ticket_requester_id_sum_7"),
        F.min("ticket_requester_id").over(w_event_id_ticket_id_by_ticket_3).alias("min_ticket_requester_id_so_far"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "agent_name",
        "event_id",
        "ticket_requester_id",
        "ticket_opened_at_2",
        "ticket_priority",
        "max_ticket_requester_id",
        "ticket_requester_id_minus_max",
        "ticket_requester_id_minus_max_dense_rank",
        "ticket_requester_id_sum_7",
        "min_ticket_requester_id_so_far",
        F.round(F.col("max_ticket_requester_id") * 1.07, 2).alias("max_ticket_requester_id_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("ticket_priority")), " ", "_")).alias("ticket_priority_slug"),
    )
    .orderBy(
        F.col("ticket_requester_id_minus_max_dense_rank").desc(),
        F.col("ticket_requester_id_sum_7").desc(),
    )
)
