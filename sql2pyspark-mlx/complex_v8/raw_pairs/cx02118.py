from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

rollup_base = (
    valid
    .filter(F.col("priority") == "P2")
    .groupBy("agent_id", F.year("closed_at").alias("closed_at_year"), "priority")
    .agg(
        F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("priority") == "P2", 1)) / F.count("*"), 1).alias("pct_priority_p2"),
        F.count("*").alias("n_rows"),
    )
)

combined_details = (
    rollup_base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.closed_at_year",
        "s.priority",
        "s.pct_priority_p2",
        "s.n_rows",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    combined_details
    .select(
        "agent_id",
        "closed_at_year",
        "priority",
        "pct_priority_p2",
        "n_rows",
        "agent_hired_on",
        "agent_tier",
    )
    .orderBy(F.col("n_rows").desc(), F.col("pct_priority_p2").desc())
)
