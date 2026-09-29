from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at = Window.partitionBy("channel").orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at")
w_channel_by_closed_at_ties = Window.partitionBy("channel").orderBy(F.col("closed_at").asc_nulls_last())

filtered_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.size(F.col("tags")) >= 2))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

combined_details_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.priority_prefix",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    combined_details_agent
    .select(
        "*",
        F.lag("closed_at").over(w_channel_by_closed_at).alias("prev_closed_at"),
        F.round(F.cume_dist().over(w_channel_agent_id_by_closed_a), 3).alias("closed_at_cume_dist"),
        F.round(F.percent_rank().over(w_channel_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "closed_at",
        "channel",
        "priority_prefix",
        "agent_name",
        "agent_tier",
        "closed_at_percent_rank",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.greatest(F.col("closed_at_percent_rank"), F.col("closed_at_cume_dist")).alias("greatest_closed_at_percent_rank_closed_at_cume_d"),
    )
    .orderBy("agent_name", "agent_tier")
)
