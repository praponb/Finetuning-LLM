from pyspark.sql import functions as F

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    joined_ranges.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

with_parent_agent = (
    with_stats.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_12h",
        "s.prior_requester_id_12h",
        "s.last_event_at",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .select(
        "id",
        "prior_rows_12h",
        "prior_requester_id_12h",
        "last_event_at",
        "agent_team",
        F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_requester_id_12h"), F.lit(0)), 3).alias("prior_rows_12h_per_prior_requester_id_12h"),
    )
    .orderBy(
        F.col("prior_rows_12h").desc(),
        F.col("last_event_at").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
