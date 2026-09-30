from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_agent_hired_on_mo = Window.partitionBy("event_type", F.month("agent_hired_on")).orderBy("ticket_opened_at", "id")
w_actor_event_type_by_ticket_o = Window.partitionBy("actor", "event_type").orderBy(F.col("ticket_opened_at").desc_nulls_first(), "id")
w_actor_ticket_priority = Window.partitionBy("actor", "ticket_priority")
w_event_type_event_at_month = Window.partitionBy("event_type", F.month("event_at"))
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_type_agent_hired_on_mo_2 = Window.partitionBy("event_type", F.month("agent_hired_on")).orderBy("ticket_opened_at", "id").rowsBetween(-2, -1)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_at_year_by_agent_hired = Window.partitionBy(F.year("event_at")).orderBy("agent_hired_on")
w_all_by_event_at_prior_3 = Window.orderBy("event_at", "id").rowsBetween(-3, -1)

cleaned = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date")) & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    joined_ticket
    .select(
        "*",
        F.dense_rank().over(w_event_type_agent_hired_on_mo).alias("ticket_opened_at_dense_rank"),
        F.lead("ticket_opened_at").over(w_actor_event_type_by_ticket_o).alias("next_ticket_opened_at"),
        F.sum("ticket_requester_id").over(w_actor_ticket_priority).alias("actor_ticket_priority_sum_ticket_requester_id"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_event_at_month), 2).alias("ticket_requester_id_vs_event_type_event_at_month"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_event_type_event_at_month), F.lit(0)), 4).alias("ticket_requester_id_share_of_event_type_event_at"),
        F.nth_value("ticket_requester_id", 2).over(w_all_by_event_at_full).alias("second_ticket_requester_id"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.sum("ticket_requester_id").over(w_event_type_agent_hired_on_mo_2).alias("ticket_requester_id_sum_2_prior"),
        F.lag("ticket_requester_id", 1, 0).over(w_ticket_id_by_event_at).alias("prev_ticket_requester_id"),
        F.lag("agent_hired_on").over(w_actor_event_type_by_ticket_o).alias("prev_agent_hired_on"),
        F.rank().over(w_event_at_year_by_agent_hired).alias("agent_hired_on_rank"),
        F.sum("ticket_requester_id").over(w_all_by_event_at_prior_3).alias("ticket_requester_id_sum_3_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_requester_id",
        "ticket_opened_at",
        "agent_hired_on",
        "next_ticket_opened_at",
        "actor_ticket_priority_sum_ticket_requester_id",
        "ticket_requester_id_vs_event_type_event_at_month",
        "ticket_requester_id_share_of_event_type_event_at",
        "second_ticket_requester_id",
        "event_at_rank",
        "ticket_requester_id_sum_2_prior",
        "prev_ticket_requester_id",
        "prev_agent_hired_on",
        "ticket_requester_id_sum_3_prior",
        (F.floor(F.col("prev_ticket_requester_id") / 5) * 5).alias("prev_ticket_requester_id_bucket"),
        F.quarter(F.col("prev_agent_hired_on")).alias("prev_agent_hired_on_quarter"),
    )
    .orderBy("actor", F.col("prev_ticket_requester_id").desc())
)
