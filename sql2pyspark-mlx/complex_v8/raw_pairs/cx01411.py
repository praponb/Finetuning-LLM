from pyspark.sql import functions as F

valid = (
    df_support_agents
    .filter((F.col("team").isin("Tech", "Onboarding")) | (F.col("tier") == "T2"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

tagged = (
    summarized_rows
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "last_ticket_opened_at",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_ticket_opened_at")).alias("days_since_last_ticket_opened_at"),
        F.date_format(F.col("first_ticket_closed_at"), "yyyy-MM").alias("first_ticket_closed_at_label"),
    )
)

result = (
    tagged
    .rollup("team", "team_or_unknown", "tier")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("G%"), 1).otherwise(0)).alias("n_upper_trim_agent_name_like_g"),
        F.countDistinct("team").alias("distinct_teams"),
    )
    .orderBy(
        F.col("team").asc_nulls_last(),
        F.col("team_or_unknown").asc_nulls_last(),
        F.col("tier").asc_nulls_last(),
    )
)
