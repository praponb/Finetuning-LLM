from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_name_len_tier_group = Window.partitionBy("agent_name_len", "tier_group")
w_tier_group_by_agent_name_len = Window.partitionBy("tier_group").orderBy(F.col("agent_name_len").asc_nulls_last(), "id")

filtered = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-03-30").cast("date")) & (F.length(F.trim(F.col("agent_name"))) >= 6))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.length(F.trim(F.col("tier"))).alias("tier_len"),
        F.lower(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

tagged = (
    filtered
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        "agent_name_clean",
        F.when(F.col("tier").isin("T3", "T1"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.abs(F.col("tier_len") - 6).alias("tier_len_distance"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.round(F.col("tier_len_distance") - F.avg("tier_len_distance").over(w_agent_name_len_tier_group), 2).alias("tier_len_distance_vs_agent_name_len_tier_group_a"),
        F.round(F.col("agent_name_len") / F.nullif(F.sum("agent_name_len").over(w_agent_name_len_tier_group), F.lit(0)), 4).alias("agent_name_len_share_of_agent_name_len_tier_grou"),
        (F.col("agent_name_len") - F.lag("agent_name_len").over(w_tier_group_by_agent_name_len)).alias("agent_name_len_change"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["team", "agent_name_len", "tier_group"], ["team"], ["agent_name_len"]],
        "team", "agent_name_len", "tier_group",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("team") == "Billing", F.col("agent_name_len")).otherwise(0)).alias("agent_name_len_team_billing"),
    )
    .select(
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "agent_name_len",
        F.coalesce(F.col("tier_group"), F.lit("ALL")).alias("tier_group"),
        "row_count",
        "agent_name_len_team_billing",
    )
)
