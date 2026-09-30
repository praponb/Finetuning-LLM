from pyspark.sql import functions as F

valid = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
        F.trunc(F.col("hired_on"), "MM").alias("hired_on_month_start"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("closed_at").alias("last_ticket_closed_at"))
)

profiled = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_ticket_closed_at")
)

result = (
    profiled
    .rollup("team_or_unknown", "tier", "tier_or_unknown")
    .agg(F.array_sort(F.collect_set("tier")).alias("tier_list"))
)
