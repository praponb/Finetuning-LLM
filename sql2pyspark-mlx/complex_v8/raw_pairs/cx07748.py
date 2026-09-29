from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_agent_teams_by_requ = Window.partitionBy("distinct_agent_teams").orderBy("requester_id_to_requester_id_bucket_ratio", "priority", "agent_team").rowsBetween(-1, -1)
w_agent_team = Window.partitionBy("agent_team")
w_distinct_agent_teams_by_requ_2 = Window.partitionBy("distinct_agent_teams").orderBy("requester_id_to_requester_id_bucket_ratio", "priority", "agent_team").rowsBetween(-2, -1)

scoped = (
    df_tickets
    .filter((F.col("csat") >= 2) & (F.col("closed_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel").isin("phone", "web")) & (F.size(F.col("t.tags")) >= 2), "left_semi")
    .filter(F.col("csat").between(2, 4))
)

combined_details_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags",
        "s.channel_or_unknown",
        "s.tags_text",
        F.col("su.team").alias("agent_team"),
    )
)

tagged = (
    combined_details_agent
    .filter(F.col("opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "*",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("channel_or_unknown")), " ", "_")).alias("channel_or_unknown_slug"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

per_group = (
    tagged
    .groupBy("priority", "agent_team")
    .agg(
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("requester_id_bucket"), F.lit(0)), 3).alias("requester_id_to_requester_id_bucket_ratio"),
    )
)

ranked = (
    per_group
    .select(
        "*",
        F.round(F.avg("distinct_agent_teams").over(w_distinct_agent_teams_by_requ), 2).alias("distinct_agent_teams_avg_1_prior"),
        F.round(F.col("distinct_agent_teams") - F.avg("distinct_agent_teams").over(w_agent_team), 2).alias("distinct_agent_teams_vs_agent_team_avg"),
        F.round(F.avg("distinct_agent_teams").over(w_distinct_agent_teams_by_requ_2), 2).alias("distinct_agent_teams_avg_2_prior"),
        F.size(F.collect_set("priority").over(w_agent_team)).alias("distinct_priority_in_agent_team"),
    )
)

tagged_2 = (
    ranked
    .filter(F.col("agent_team") != "Onboarding")
    .select(
        "priority",
        "agent_team",
        "distinct_agent_teams",
        "distinct_agent_teams_avg_1_prior",
        "distinct_agent_teams_vs_agent_team_avg",
        "distinct_agent_teams_avg_2_prior",
        "distinct_priority_in_agent_team",
        F.abs(F.col("distinct_agent_teams") - 5).alias("distinct_agent_teams_distance"),
    )
)

result = (
    tagged_2
    .groupingSets(
        [["distinct_agent_teams", "distinct_priority_in_agent_team", "priority"], ["distinct_agent_teams"], []],
        "distinct_agent_teams", "distinct_priority_in_agent_team", "priority",
    )
    .agg(
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.sum(F.when(~F.col("agent_team").isin("Onboarding", "Billing"), 1).otherwise(0)).alias("n_agent_team_not_in_onboarding"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .orderBy(
        F.col("distinct_agent_teams").asc_nulls_last(),
        F.col("distinct_priority_in_agent_team").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
