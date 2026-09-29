from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_actor = Window.partitionBy("ticket_priority", "actor")
w_ticket_priority_by_ticket_op = Window.partitionBy("ticket_priority").orderBy(F.col("ticket_opened_at").desc())
w_ticket_priority_by_ticket_op_2 = Window.partitionBy("ticket_priority").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_agent_tier_by_ticket_request = Window.partitionBy("agent_tier").orderBy(F.col("ticket_requester_id").desc(), "id")

scoped_events = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.col("event_type") != "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

enriched = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_priority_actor).alias("rows_in_ticket_priority_actor"),
        F.max("ticket_requester_id").over(w_ticket_priority_actor).alias("ticket_priority_actor_max_ticket_requester_id"),
        F.size(F.collect_set("ticket_id").over(w_ticket_priority_actor)).alias("distinct_ticket_id_in_ticket_priority_actor"),
        F.round(F.percent_rank().over(w_ticket_priority_by_ticket_op), 3).alias("ticket_opened_at_percent_rank"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_priority_by_ticket_op_2), 2).alias("ticket_requester_id_avg_7"),
        F.rank().over(w_ticket_priority_by_ticket_op).alias("ticket_opened_at_rank"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_priority_actor), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_priority_act"),
        F.lead("ticket_opened_at").over(w_agent_tier_by_ticket_request).alias("next_ticket_opened_at"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("ticket_opened_at_rank"), F.lit(0)).alias("ticket_opened_at_rank_filled"),
        F.abs(F.col("ticket_requester_id") - 16).alias("ticket_requester_id_distance"),
        F.date_trunc("hour", F.col("next_ticket_opened_at")).alias("next_ticket_opened_at_hour"),
    )
)

result = (
    labeled
    .filter(F.col("event_at") < F.lit("2025-03-02 15:00:00").cast("timestamp"))
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_slug",
        "ticket_requester_id",
        "ticket_opened_at",
        "ticket_priority",
        "agent_tier",
        "rows_in_ticket_priority_actor",
        "distinct_ticket_id_in_ticket_priority_actor",
        "ticket_opened_at_percent_rank",
        "ticket_requester_id_avg_7",
        "ticket_opened_at_rank",
        "ticket_requester_id_share_of_ticket_priority_act",
        "next_ticket_opened_at",
        "ticket_opened_at_rank_filled",
        "next_ticket_opened_at_hour",
        F.concat(F.col("event_type_slug"), F.lit("#"), F.col("ticket_opened_at_rank").cast("string")).alias("event_type_slug_ref"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)
