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

filtered = (
    in_window
    .crossJoin(in_window.agg(F.min("prior_requester_id_7d").alias("overall_min_prior_requester_id_7d")))
    .filter(F.col("prior_requester_id_7d") > F.col("overall_min_prior_requester_id_7d"))
    .drop("overall_min_prior_requester_id_7d")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

profiled = (
    filtered.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_closed_at",
    )
)

with_parent_agent = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_requester_id_7d",
        "s.first_ticket_closed_at",
        "su.agent_name",
    )
)

result = (
    with_parent_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_requester_id_7d",
        "first_ticket_closed_at",
        "agent_name",
    )
    .orderBy(F.col("agent_id").desc(), "id")
    .limit(5)
)
