from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("ticket_csat")
w_agent_hired_on_year_ticket_i = Window.partitionBy(F.year("agent_hired_on"), "ticket_id")
w_all_by_ticket_requester_id_t = Window.orderBy(F.col("ticket_requester_id").desc())
w_all_by_agent_hired_on_prior = Window.orderBy("agent_hired_on", "id").rowsBetween(-6, -1)
w_event_at_to_date_event_type_2 = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("ticket_csat", "id")
w_all_by_ticket_requester_id_r = Window.orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_event_type_next3_ticket_csat = Window.partitionBy("event_type", "next3_ticket_csat")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").desc())
w_distinct_ticket_priority_in = Window.partitionBy("distinct_ticket_priority_in_agent_hired_on_year_").orderBy(F.col("ticket_csat_vs_agent_hired_on_year_ticket_id_avg").desc(), "id").rowsBetween(-1, Window.currentRow)
w_distinct_ticket_priority_in_2 = Window.partitionBy("distinct_ticket_priority_in_agent_hired_on_year_").orderBy(F.col("ticket_csat_vs_agent_hired_on_year_ticket_id_avg").desc(), "id").rowsBetween(-2, -1)
w_distinct_ticket_priority_in_3 = Window.partitionBy("distinct_ticket_priority_in_agent_hired_on_year_").orderBy(F.col("ticket_csat_vs_agent_hired_on_year_ticket_id_avg").desc())
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter((F.col("event_type") == "created") & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    joined_ticket
    .select(
        "*",
        F.dense_rank().over(w_event_at_to_date_event_type).alias("ticket_csat_dense_rank"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_agent_hired_on_year_ticket_i), 2).alias("ticket_csat_vs_agent_hired_on_year_ticket_id_avg"),
        F.round(F.cume_dist().over(w_all_by_ticket_requester_id_t), 3).alias("ticket_requester_id_cume_dist"),
        F.count(F.lit(1)).over(w_agent_hired_on_year_ticket_i).alias("rows_in_agent_hired_on_year_ticket_id"),
        F.size(F.collect_set("ticket_priority").over(w_agent_hired_on_year_ticket_i)).alias("distinct_ticket_priority_in_agent_hired_on_year_"),
        F.round(F.percent_rank().over(w_all_by_ticket_requester_id_t), 3).alias("ticket_requester_id_percent_rank"),
        F.round(F.avg("ticket_csat").over(w_all_by_agent_hired_on_prior), 2).alias("ticket_csat_avg_6_prior"),
        F.lead("ticket_csat", 2).over(w_event_at_to_date_event_type_2).alias("next2_ticket_csat"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_agent_hired_on_year_ticket_i), F.lit(0)), 4).alias("ticket_requester_id_share_of_agent_hired_on_year"),
        F.first("event_at", ignorenulls=True).over(w_all_by_ticket_requester_id_r).alias("first_known_event_at"),
        F.lead("ticket_csat", 3).over(w_all_by_agent_hired_on).alias("next3_ticket_csat"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("ticket_csat_dense_rank") <= 5) & (F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
)

metrics = (
    first_rows
    .select(
        "*",
        F.sum("ticket_csat_avg_6_prior").over(w_event_type_next3_ticket_csat).alias("event_type_next3_ticket_csat_sum_ticket_csat_avg"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.avg("ticket_csat_dense_rank").over(w_distinct_ticket_priority_in), 2).alias("ticket_csat_dense_rank_avg_2"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_next3_ticket_csat), 2).alias("ticket_requester_id_vs_event_type_next3_ticket_c"),
        F.sum("next2_ticket_csat").over(w_distinct_ticket_priority_in_2).alias("next2_ticket_csat_sum_2_prior"),
        F.round(F.percent_rank().over(w_distinct_ticket_priority_in_3), 3).alias("ticket_csat_vs_agent_hired_on_year_ticket_id_avg_2"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.first("next2_ticket_csat").over(w_actor_by_event_at_full).alias("first_next2_ticket_csat"),
    )
)

result = (
    metrics
    .filter(F.col("ticket_id") >= 27)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_requester_id",
        "ticket_priority",
        "agent_name",
        "ticket_csat_vs_agent_hired_on_year_ticket_id_avg",
        "ticket_requester_id_cume_dist",
        "rows_in_agent_hired_on_year_ticket_id",
        "ticket_requester_id_percent_rank",
        "ticket_csat_avg_6_prior",
        "next2_ticket_csat",
        "ticket_requester_id_share_of_agent_hired_on_year",
        "first_known_event_at",
        "next3_ticket_csat",
        "event_type_next3_ticket_csat_sum_ticket_csat_avg",
        "event_at_cume_dist",
        "ticket_csat_dense_rank_avg_2",
        "next2_ticket_csat_sum_2_prior",
        "event_at_cume_dist_2",
        "first_next2_ticket_csat",
    )
    .orderBy("event_at_cume_dist_2", "ticket_requester_id_cume_dist")
)
