from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_opened_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter(F.col("channel").isin("email", "web", "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

enriched_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

labeled = (
    enriched_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "agent_tier",
        "agent_hired_on",
        F.substring(F.trim(F.col("agent_tier")), 1, 2).alias("agent_tier_prefix"),
        F.abs(F.col("csat") - 3).alias("csat_distance"),
        F.substring(F.trim(F.col("agent_tier")), 1, 4).alias("agent_tier_prefix_2"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.round(F.avg("csat").over(w_csat), 2).alias("csat_avg_csat"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.max("csat").over(w_csat).alias("csat_max_csat"),
        F.last("requester_id").over(w_opened_at_month_agent_id_by).alias("last_requester_id"),
    )
)

with_children_events = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.agent_tier",
        "s.agent_hired_on",
        "s.csat_distance",
        "s.agent_tier_prefix_2",
        "s.csat_avg_csat",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count_2"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    with_children_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_actor_count_2"), F.lit(0)).alias("event_actor_count_2"),
        "t.last_event_at",
    )
)

tagged = (
    summarized_rows
    .filter(F.col("event_actor_count") > 1)
    .select(
        "id",
        "opened_at",
        "csat_distance",
        "agent_tier_prefix_2",
        "csat_avg_csat",
        "event_id",
        "event_actor",
        "event_at",
        "event_type",
        "event_actor_count_2",
        "last_event_at",
        F.when(F.col("event_actor").isin("agent", "customer"), "group_a").when(F.col("event_actor") == "system", "group_b").otherwise("other").alias("event_actor_group"),
        F.abs(F.col("event_actor_count_2") - 4).alias("event_actor_count_2_distance"),
        F.coalesce(F.col("event_actor_count_2"), F.lit(0)).alias("event_actor_count_2_filled"),
    )
)

result = (
    tagged
    .select(
        "id",
        "opened_at",
        "csat_distance",
        "event_id",
        "event_actor",
        "event_at",
        "event_actor_group",
        "event_actor_count_2_distance",
        "event_actor_count_2_filled",
        F.greatest(F.col("event_actor_count_2"), F.col("csat_distance")).alias("greatest_event_actor_count_2_csat_distance"),
    )
    .orderBy(F.col("csat_distance").asc_nulls_last(), "id", "event_id")
    .limit(10)
)
