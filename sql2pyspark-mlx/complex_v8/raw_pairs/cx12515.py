from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_closed_at = Window.orderBy("closed_at", "id")

scoped = (
    df_tickets
    .filter((F.col("requester_id") > 3) & (F.col("closed_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "su.agent_name",
    )
)

metrics = (
    enriched
    .withColumn("rows_in_priority", F.count(F.lit(1)).over(w_priority))
    .withColumn("closed_at_percent_rank", F.round(F.percent_rank().over(w_all_by_closed_at), 3))
)

with_details_agent = (
    metrics.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.agent_name",
        "s.closed_at_percent_rank",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

agent_tier_stats = (
    with_details_agent
    .groupBy("agent_tier")
    .agg(F.percentile_approx("closed_at_percent_rank", 0.5).alias("median_closed_at_percent_rank"))
)

outliers = (
    with_details_agent.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.closed_at_percent_rank") >= F.col("g.median_closed_at_percent_rank"))
    .select(
        "s.*",
        "g.median_closed_at_percent_rank",
        F.round(F.col("s.closed_at_percent_rank") - F.col("g.median_closed_at_percent_rank"), 2).alias("closed_at_percent_rank_minus_median"),
    )
)

grouped = (
    outliers
    .groupBy("channel", F.year("closed_at").alias("closed_at_year"), "agent_team")
    .agg(
        F.count("*").alias("cnt"),
        F.min("closed_at_percent_rank_minus_median").alias("min_closed_at_percent_rank_minus_median"),
    )
)

result = (
    grouped
    .select(
        "channel",
        "agent_team",
        "cnt",
        "min_closed_at_percent_rank_minus_median",
        (F.col("closed_at_year") + F.col("cnt")).alias("closed_at_year_plus_cnt"),
    )
    .orderBy("closed_at_year_plus_cnt")
)
