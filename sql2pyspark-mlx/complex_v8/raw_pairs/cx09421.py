from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_channel = Window.partitionBy("actor", "ticket_channel")
w_ticket_channel_by_ticket_req = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_requester_id").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_agent_tier_agent_team_by_tic = Window.partitionBy("agent_tier", "agent_team").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter((~F.col("event_type").isin("assigned", "resolved")) & (F.col("actor") != "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

windowed = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_channel",
        "ticket_opened_at",
        "agent_team",
        "agent_tier",
        F.sum("ticket_requester_id").over(w_actor_ticket_channel).alias("actor_ticket_channel_sum_ticket_requester_id"),
        F.size(F.collect_set("agent_tier").over(w_actor_ticket_channel)).alias("distinct_agent_tier_in_actor_ticket_channel"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_channel_by_ticket_req), 2).alias("ticket_requester_id_avg_3"),
        F.nth_value("ticket_requester_id", 2).over(w_agent_tier_agent_team_by_tic).alias("second_ticket_requester_id"),
    )
)

stats = (
    windowed
    .filter(F.col("actor") != "customer")
    .groupBy(
        "actor",
        "agent_team",
        F.year("ticket_opened_at").alias("ticket_opened_at_year"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.min("second_ticket_requester_id").alias("min_second_ticket_requester_id"),
        F.countDistinct("ticket_channel").alias("distinct_ticket_channels"),
        F.sum(F.when(F.col("agent_team").isin("Onboarding", "Tech", "Billing"), 1).otherwise(0)).alias("n_agent_team_in_onboarding_tec"),
    )
)

result = (
    stats
    .select(
        "actor",
        "agent_team",
        "ticket_opened_at_year",
        "row_count",
        "agent_team_list",
        "distinct_agent_tiers",
        "min_second_ticket_requester_id",
        "distinct_ticket_channels",
        "n_agent_team_in_onboarding_tec",
    )
    .orderBy(
        F.col("agent_team").desc_nulls_first(),
        F.col("actor").desc(),
        "ticket_opened_at_year",
    )
    .limit(20)
)
