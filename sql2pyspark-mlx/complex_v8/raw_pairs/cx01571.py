from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_agent_tier_ticket_id = Window.partitionBy("agent_tier", "ticket_id")
w_ticket_priority_actor_by_eve = Window.partitionBy("ticket_priority", "actor").orderBy("event_at", "id")
w_cnt_actor = Window.partitionBy("cnt", "actor")
w_actor_cnt_by_stddev_ticket_r = Window.partitionBy("actor", "cnt").orderBy("stddev_ticket_requester_id", "actor")

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

with_details_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_prefix",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_details_ticket
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "ticket_tags",
        "ticket_requester_id",
        "ticket_priority",
        "agent_team",
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_event_type), F.lit(0)), 4).alias("ticket_requester_id_share_of_event_type"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_agent_tier_ticket_id), 2).alias("ticket_requester_id_vs_agent_tier_ticket_id_avg"),
        F.ntile(5).over(w_ticket_priority_actor_by_eve).alias("event_at_quintile"),
        F.size(F.collect_set("actor").over(w_event_type)).alias("distinct_actor_in_event_type"),
        F.max("ticket_requester_id").over(w_agent_tier_ticket_id).alias("agent_tier_ticket_id_max_ticket_requester_id"),
        F.sum("ticket_requester_id").over(w_agent_tier_ticket_id).alias("agent_tier_ticket_id_sum_ticket_requester_id"),
    )
)

rollup_base = (
    metrics
    .groupBy("actor")
    .agg(
        F.round(F.stddev("ticket_requester_id"), 2).alias("stddev_ticket_requester_id"),
        F.round(F.stddev("agent_tier_ticket_id_sum_ticket_requester_id"), 2).alias("stddev_agent_tier_ticket_id_sum_ticket_requester"),
        F.count("*").alias("cnt"),
        F.min("ticket_requester_id_share_of_event_type").alias("min_ticket_requester_id_share_of_event_type"),
        F.round(F.avg("distinct_actor_in_event_type"), 2).alias("avg_distinct_actor_in_event_type"),
    )
)

sequenced = (
    rollup_base
    .select(
        "actor",
        "stddev_ticket_requester_id",
        "stddev_agent_tier_ticket_id_sum_ticket_requester",
        "cnt",
        (F.col("cnt") - F.lag("cnt").over(w_actor_cnt_by_stddev_ticket_r)).alias("cnt_change"),
        F.round(F.col("stddev_ticket_requester_id") / F.nullif(F.sum("stddev_ticket_requester_id").over(w_cnt_actor), F.lit(0)), 4).alias("stddev_ticket_requester_id_share_of_cnt_actor"),
        F.sum("cnt").over(w_cnt_actor).alias("cnt_actor_sum_cnt"),
    )
)

result = (
    sequenced
    .select(
        "actor",
        "stddev_ticket_requester_id",
        "stddev_agent_tier_ticket_id_sum_ticket_requester",
        "cnt_change",
        "stddev_ticket_requester_id_share_of_cnt_actor",
        "cnt_actor_sum_cnt",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
    .orderBy(
        F.col("cnt_actor_sum_cnt").desc_nulls_first(),
        F.col("stddev_ticket_requester_id_share_of_cnt_actor").asc_nulls_last(),
    )
)
