from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_minutes_opened_at_to = Window.orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_channel_by_requeste = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id").desc(), "id")
w_agent_tier_channel_by_minute = Window.partitionBy("agent_tier", "channel").orderBy("minutes_opened_at_to_closed_at", "id")
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_tier_channel_by_minute_2 = Window.partitionBy("agent_tier", "channel").orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(-3, -1)
w_agent_tier_channel_by_minute_3 = Window.partitionBy("agent_tier", "channel").orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-01-31").cast("date")) | (~F.col("channel").isin("phone", "email")))
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
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T1")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.minutes_opened_at_to_closed_at",
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.first("requester_id").over(w_all_by_minutes_opened_at_to).alias("first_requester_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_priority_channel_by_requeste))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.cume_dist().over(w_agent_tier_channel_by_minute), 3).alias("minutes_opened_at_to_closed_at_cume_dist"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_tier), 2).alias("requester_id_vs_agent_tier_avg"),
        F.round(F.avg("requester_id").over(w_agent_tier_channel_by_minute_2), 2).alias("requester_id_avg_3_prior"),
        F.round(F.avg("requester_id").over(w_agent_tier), 2).alias("agent_tier_avg_requester_id"),
        F.round(F.avg("requester_id").over(w_agent_tier_channel_by_minute_3), 2).alias("requester_id_avg_3"),
        F.lag("requester_id").over(w_agent_tier_channel_by_minute).alias("prev_requester_id"),
    )
)

labeled = (
    ranked
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "minutes_opened_at_to_closed_at",
        "agent_tier",
        "first_requester_id",
        "minutes_opened_at_to_closed_at_cume_dist",
        "agent_tier_avg_requester_id",
        "requester_id_avg_3",
        "prev_requester_id",
        F.when(F.col("priority").isin("P4", "P3"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

result = (
    labeled
    .filter(F.col("requester_id_avg_3") <= 20.2)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "minutes_opened_at_to_closed_at",
        "agent_tier",
        "first_requester_id",
        "minutes_opened_at_to_closed_at_cume_dist",
        "requester_id_avg_3",
        "priority_group",
        "opened_at_month",
        F.round(F.col("requester_id_avg_3") * 0.25, 2).alias("requester_id_avg_3_adj"),
        F.least(F.col("requester_id"), F.col("first_requester_id")).alias("least_requester_id_first_requester_id"),
    )
    .orderBy(F.col("opened_at").desc(), "opened_at_month", "id")
    .limit(20)
)
