from pyspark.sql import functions as F

filtered = df_ticket_events.filter(F.col("event_type") == "replied")

with_parent = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.team").alias("agent_team"),
    )
)

derived = (
    with_parent
    .filter(F.col("event_type") == "resolved")
    .select("*", F.length(F.trim(F.col("actor"))).alias("actor_len"))
)

annotated = (
    derived
    .select(
        "*",
        F.upper(F.trim(F.col("agent_team"))).alias("agent_team_clean"),
        F.least(F.col("ticket_csat"), F.col("actor_len")).alias("least_ticket_csat_actor_len"),
    )
)

result = (
    annotated
    .groupingSets(
        [["ticket_id", "ticket_csat"], ["ticket_id"], []],
        "ticket_id", "ticket_csat",
    )
    .agg(
        F.round(F.sum("ticket_csat") / F.nullif(F.sum("actor_len"), F.lit(0)), 3).alias("ticket_csat_to_actor_len_ratio"),
        F.count("*").alias("n_rows"),
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_csat").asc_nulls_last(),
    )
)
