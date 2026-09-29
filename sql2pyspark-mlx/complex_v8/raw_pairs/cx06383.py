from pyspark.sql import functions as F

in_window = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.requester_id").alias("prior_requester_id_7d"),
    )
)

joined_agent = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_7d",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    joined_agent
    .select(
        "id",
        "opened_at",
        "agent_tier",
        "agent_name",
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
    .orderBy(F.col("agent_name").asc(), F.col("id").asc_nulls_last())
)
