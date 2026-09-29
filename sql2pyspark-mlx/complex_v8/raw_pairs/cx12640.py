from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.col("csat").between(2, 4)) & (F.col("channel") == "email"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
    )
)

stats = (
    recent_tickets
    .groupBy(
        "agent_id",
        "priority",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("priority") == "P4", 1)) / F.count("*"), 1).alias("pct_priority_p4"),
        F.count(F.when(F.col("priority") == "P2", 1)).alias("n_priority_p2"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
)

combined_details_agent = (
    stats.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Tech"), "left")
    .select(
        "s.agent_id",
        "s.priority",
        "s.opened_at_month",
        "s.pct_priority_p4",
        "s.n_priority_p2",
        "s.distinct_prioritys",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

scored = (
    combined_details_agent
    .withColumn("agent_name_ref", F.concat(F.col("agent_name"), F.lit("#"), F.col("agent_id").cast("string")))
)

result = (
    scored
    .filter(F.col("agent_team") == "Onboarding")
    .select(
        "agent_id",
        "priority",
        "opened_at_month",
        "pct_priority_p4",
        "distinct_prioritys",
        "agent_team",
        "agent_hired_on",
        "agent_name",
        "agent_name_ref",
    )
)
