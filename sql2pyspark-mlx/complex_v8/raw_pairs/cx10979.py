from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_requester_id_tier_channel = Window.partitionBy("requester_id_tier", "channel")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")

prepared = (
    df_tickets
    .filter((F.col("csat") <= 5) & (F.array_contains(F.col("tags"), "bug")))
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
        F.when(F.col("requester_id") >= 14, "large").when(F.col("requester_id") >= 8, "medium").otherwise("small").alias("requester_id_tier"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
        F.year(F.col("opened_at")).alias("opened_at_year"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_requester_id_tier_channel)).alias("distinct_agent_id_in_requester_id_tier_channel"),
        F.round(F.percent_rank().over(w_all_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
    )
)

qualified = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
    .filter(F.col("agent_id") <= 4)
)

combined_details = (
    qualified.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team").isin("Tech", "Onboarding"))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_prefix",
        "s.opened_at_year",
        "s.opened_at_percent_rank",
        "s.opened_at_rank",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

agent_id_stats = (
    combined_details
    .groupBy("agent_id")
    .agg(F.percentile_approx("opened_at_percent_rank", 0.5).alias("median_opened_at_percent_rank"))
)

compared = (
    combined_details.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.opened_at_percent_rank") >= F.col("g.median_opened_at_percent_rank"))
    .select(
        "s.*",
        "g.median_opened_at_percent_rank",
        F.round(F.col("s.opened_at_percent_rank") - F.col("g.median_opened_at_percent_rank"), 2).alias("opened_at_percent_rank_minus_median"),
    )
)

stats = (
    compared
    .groupBy(
        "agent_id",
        F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"),
        F.year("opened_at").alias("opened_at_year_2"),
    )
    .agg(
        F.round(F.sum("opened_at_percent_rank_minus_median"), 2).alias("total_opened_at_percent_rank_minus_median"),
        F.min("opened_at_year").alias("min_opened_at_year"),
        F.max("opened_at_rank").alias("max_opened_at_rank"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
    )
)

result = (
    stats
    .select(
        "agent_id",
        "agent_hired_on_ym",
        "min_opened_at_year",
        "max_opened_at_rank",
        "avg_csat",
        "agent_team_list",
        F.initcap(F.trim(F.col("agent_hired_on_ym"))).alias("agent_hired_on_ym_clean"),
        F.substring(F.trim(F.col("agent_hired_on_ym")), 1, 2).alias("agent_hired_on_ym_prefix"),
    )
    .orderBy("max_opened_at_rank")
)
