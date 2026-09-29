from pyspark.sql import functions as F

covered = (
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

agent_id_stats = (
    covered
    .groupBy("agent_id")
    .agg(F.avg("prior_rows_3d").alias("avg_prior_rows_3d"))
)

outliers = (
    covered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_rows_3d") > F.col("g.avg_prior_rows_3d"))
    .select(
        "s.*",
        "g.avg_prior_rows_3d",
        F.round(F.col("s.prior_rows_3d") - F.col("g.avg_prior_rows_3d"), 2).alias("prior_rows_3d_minus_avg"),
    )
)

detail_rows_events = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.month(F.col("t.event_at")).isin(1, 2)), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.avg_prior_rows_3d",
        "s.prior_rows_3d_minus_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

with_parent = (
    detail_rows_events.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_3d_minus_avg",
        "s.event_id",
        "s.event_actor",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["agent_team", "event_id"], ["agent_team"]],
        "agent_team", "event_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("prior_rows_3d_minus_avg"), 2).alias("total_prior_rows_3d_minus_avg"),
    )
)
