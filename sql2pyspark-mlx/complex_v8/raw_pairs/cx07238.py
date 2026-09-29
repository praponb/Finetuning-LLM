from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_priority_len_full = Window.partitionBy("channel").orderBy("priority_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tags_count_by_days_since_ope = Window.partitionBy("tags_count").orderBy(F.col("days_since_opened_at").desc(), "id")

valid = (
    df_tickets
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

agent_id_stats = (
    valid
    .groupBy("agent_id")
    .agg(F.avg("priority_len").alias("avg_priority_len"))
)

compared = (
    valid.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.priority_len") > F.col("g.avg_priority_len"))
    .select(
        "s.*",
        "g.avg_priority_len",
        F.round(F.col("s.priority_len") - F.col("g.avg_priority_len"), 2).alias("priority_len_minus_avg"),
    )
)

ranked = (
    compared
    .select(
        "*",
        F.last("tags_count").over(w_channel_by_priority_len_full).alias("last_tags_count"),
        F.lead("priority_len").over(w_tags_count_by_days_since_ope).alias("next_priority_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    ranked.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_len",
        "s.days_since_opened_at",
        "s.avg_priority_len",
        "s.last_tags_count",
        "s.next_priority_len",
        "s.ticket_count",
        F.col("su.team").alias("agent_team"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 5)
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.priority_len",
        "s.days_since_opened_at",
        "s.last_tags_count",
        "s.next_priority_len",
        "s.ticket_count",
        "s.agent_team",
        F.col("su.team").alias("agent_team_2"),
        "su.agent_name",
    )
)

result = (
    joined
    .select(
        "id",
        "channel",
        "tags",
        "priority_len",
        "next_priority_len",
        "ticket_count",
        "agent_team",
    )
    .orderBy(F.col("channel").desc_nulls_first(), F.col("next_priority_len").asc())
)
