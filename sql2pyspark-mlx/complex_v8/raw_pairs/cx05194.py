from pyspark.sql import functions as F

recent_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.concat_ws("|", F.col("tier"), F.col("team")).alias("tier_team_label"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.min("opened_at").alias("first_ticket_opened_at_2"),
    )
)

with_stats = (
    recent_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_ticket_opened_at", "t.first_ticket_opened_at_2")
)

exploded_detail_tickets = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.hired_on_label",
        "s.first_ticket_opened_at",
        "s.first_ticket_opened_at_2",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

derived = (
    exploded_detail_tickets
    .select(
        "*",
        F.abs(F.col("ticket_csat") - 4).alias("ticket_csat_distance"),
        F.lower(F.trim(F.col("hired_on_label"))).alias("hired_on_label_clean"),
    )
)

line_level_tickets = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.hired_on_label",
        "s.ticket_id",
        "s.ticket_csat",
        "s.ticket_csat_distance",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    line_level_tickets
    .filter(F.col("ticket_id") >= 44)
    .select(
        "id",
        "hired_on_label",
        "ticket_id",
        "ticket_csat",
        "ticket_csat_distance",
        "ticket_id_2",
        "ticket_closed_at_2",
        "ticket_priority",
        F.substring(F.trim(F.col("ticket_priority")), 1, 3).alias("ticket_priority_prefix"),
        (F.col("ticket_csat") + F.col("ticket_csat_distance")).alias("ticket_csat_plus_ticket_csat_distance"),
    )
    .orderBy(F.col("ticket_csat_distance").desc())
)
