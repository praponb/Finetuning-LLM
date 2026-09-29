from pyspark.sql import functions as F

base = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
    )
)

with_parent_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        F.col("su.team").alias("agent_team"),
    )
)

per_group = (
    with_parent_agent
    .filter(F.col("agent_id").isNotNull())
    .groupBy(F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.sum(F.when(F.col("agent_team") == "VIP", 1).otherwise(0)).alias("n_agent_team_vip"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.count("*").alias("row_count"),
        F.min("opened_at").alias("first_opened_at"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.min("distinct_agent_ids").alias("overall_min_distinct_agent_ids")))
    .filter(F.col("distinct_agent_ids") > F.col("overall_min_distinct_agent_ids"))
    .drop("overall_min_distinct_agent_ids")
)

without_matches_2 = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    without_matches_2
    .select(
        "opened_at_ym",
        "agent_team_list",
        "distinct_agent_ids",
        "row_count",
    )
    .orderBy(F.col("opened_at_ym").desc(), F.col("row_count").desc_nulls_first())
)
