from pyspark.sql import functions as F

filtered_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.concat_ws("-", F.col("agent_name"), F.col("team")).alias("agent_name_team_label"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
    )
)

exploded_detail = (
    filtered_agents.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.hired_on_label",
        F.col("t.id").alias("ticket_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

with_details_ticket = (
    exploded_detail.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_details_ticket
    .groupingSets(
        [["ticket_csat", "ticket_agent_id", "ticket_id"], ["ticket_csat"], []],
        "ticket_csat", "ticket_agent_id", "ticket_id",
    )
    .agg(
        F.grouping("ticket_csat").alias("is_total_ticket_csat"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("ticket_csat").asc_nulls_last(),
        F.col("ticket_agent_id").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
