from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.requester_id").alias("prior_requester_id_1d"),
    )
)

with_parent_agent = (
    covered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_1d",
        "s.prior_requester_id_1d",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_parent = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_requester_id_1d",
        "s.agent_name",
        "s.agent_tier",
        "s.agent_team",
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

grouped = (
    with_parent
    .groupBy("agent_id", "agent_tier")
    .agg(
        F.min("prior_requester_id_1d").alias("min_prior_requester_id_1d"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("K%"), F.col("prior_requester_id_1d")).otherwise(0)).alias("prior_requester_id_1d_upper_trim_agent_name_like"),
        F.sum("prior_requester_id_1d").alias("total_prior_requester_id_1d"),
        F.percentile_approx("prior_requester_id_1d", 0.5).alias("median_prior_requester_id_1d"),
        F.avg("prior_requester_id_1d").alias("avg_prior_requester_id_1d_check"),
    )
    .filter(F.col("avg_prior_requester_id_1d_check") > 12)
    .drop("avg_prior_requester_id_1d_check")
)

result = (
    grouped
    .select(
        "agent_id",
        "agent_tier",
        "total_prior_requester_id_1d",
        "prior_requester_id_1d_upper_trim_agent_name_like",
        "min_prior_requester_id_1d",
    )
    .unpivot(
        ids=["agent_id", "agent_tier"],
        values=["total_prior_requester_id_1d", "prior_requester_id_1d_upper_trim_agent_name_like", "min_prior_requester_id_1d"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("agent_id", "agent_tier", "measure")
)
