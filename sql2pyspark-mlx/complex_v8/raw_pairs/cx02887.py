from pyspark.sql import functions as F

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

line_level = (
    matched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("escalated", "created", "replied")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.prior_rows_3d",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

combined_details = (
    line_level.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.prior_rows_3d",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

tagged = (
    combined_details
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        "event_id",
        "event_type",
        "event_actor",
        "agent_name",
        "agent_team",
        F.concat_ws("|", F.col("agent_team"), F.col("agent_name")).alias("agent_team_agent_name_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_actor")), " ", "_")).alias("event_actor_slug"),
        F.when(F.col("prior_rows_3d") >= 7, "HIGH").when(F.col("prior_rows_3d") >= 3, "MEDIUM").otherwise("LOW").alias("prior_rows_3d_tier"),
    )
)

result = (
    tagged
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        "event_id",
        "agent_name",
        "agent_team",
        "agent_team_agent_name_label",
        "event_actor_slug",
        "prior_rows_3d_tier",
    )
    .orderBy(F.col("agent_name").desc())
)
