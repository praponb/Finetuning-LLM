from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_by_event_at = Window.partitionBy("agent_team").orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_csat = Window.orderBy(F.col("ticket_csat").desc(), "id")
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_event_type_ticket_priority = Window.partitionBy("event_type", "ticket_priority")
w_agent_team_by_event_at_ties = Window.partitionBy("agent_team").orderBy(F.col("event_at").desc())
w_all_by_ticket_requester_id_t = Window.orderBy(F.col("ticket_requester_id").desc())
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_moving_2 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

joined_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_ref",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    joined_ticket
    .select(
        "*",
        F.lead("event_at").over(w_agent_team_by_event_at).alias("next_event_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_csat), 3).alias("ticket_csat_percent_rank"),
        F.count(F.lit(1)).over(w_actor_event_type).alias("rows_in_actor_event_type"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_ticket_priority), 2).alias("ticket_requester_id_vs_event_type_ticket_priorit"),
        F.lag("ticket_requester_id", 3, 0).over(w_agent_team_by_event_at).alias("prev3_ticket_requester_id"),
        F.round(F.cume_dist().over(w_agent_team_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_ticket_requester_id_t).alias("ticket_requester_id_dense_rank"),
        F.max("ticket_requester_id").over(w_actor_event_type).alias("actor_event_type_max_ticket_requester_id"),
        F.row_number().over(w_agent_team_by_event_at).alias("seq_no"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_event_type_ticket_priority), 2).alias("ticket_csat_vs_event_type_ticket_priority_avg"),
        F.nth_value("ticket_csat", 2).over(w_all_by_event_at_full).alias("second_ticket_csat"),
        F.max("ticket_requester_id").over(w_all_by_event_at_moving_2).alias("ticket_requester_id_max_3"),
        F.size(F.collect_set("agent_tier").over(w_actor_event_type)).alias("distinct_agent_tier_in_actor_event_type"),
        F.count(F.lit(1)).over(w_event_type_ticket_priority).alias("rows_in_event_type_ticket_priority"),
        F.size(F.collect_set("agent_team").over(w_event_type_ticket_priority)).alias("distinct_agent_team_in_event_type_ticket_priorit"),
    )
)

leaders = (
    with_trends
    .filter(F.col("seq_no") <= 2)
    .drop("seq_no")
)

result = (
    leaders
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_priority",
        "ticket_csat",
        "ticket_requester_id",
        "next_event_at",
        "ticket_csat_percent_rank",
        "rows_in_actor_event_type",
        "ticket_requester_id_vs_event_type_ticket_priorit",
        "event_at_cume_dist",
        "ticket_requester_id_dense_rank",
        "ticket_csat_vs_event_type_ticket_priority_avg",
        "second_ticket_csat",
        "ticket_requester_id_max_3",
        "distinct_agent_tier_in_actor_event_type",
        "rows_in_event_type_ticket_priority",
        "distinct_agent_team_in_event_type_ticket_priorit",
        F.least(F.col("event_at_cume_dist"), F.col("ticket_csat_percent_rank")).alias("least_event_at_cume_dist_ticket_csat_percent_ran"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
    .orderBy(
        "ticket_priority",
        F.col("distinct_agent_team_in_event_type_ticket_priorit").desc(),
    )
)
