from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor = Window.partitionBy("event_type", "actor")
w_all_by_ticket_closed_at_runn = Window.orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_requester_id = Window.partitionBy("actor").orderBy("ticket_requester_id", "id")
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_all_by_agent_hired_on_full = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_ticket_requester_id_2 = Window.partitionBy("actor").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_ticket_c = Window.partitionBy("actor", "event_type").orderBy("ticket_closed_at", "id").rowsBetween(-1, -1)
w_all_by_agent_hired_on_moving = Window.orderBy("agent_hired_on", "id").rowsBetween(-2, Window.currentRow)
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id")
w_event_type = Window.partitionBy("event_type")
w_ticket_id_seq_no_2_by_ticket = Window.partitionBy("ticket_id", "seq_no_2").orderBy("ticket_closed_at", "id")
w_ticket_id_ticket_closed_at_t = Window.partitionBy("ticket_id", F.to_date("ticket_closed_at"))
w_all_by_ticket_requester_id_m = Window.orderBy(F.col("ticket_requester_id_max_1_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_requester_id_m_2 = Window.orderBy(F.col("ticket_requester_id_max_1_prior").desc())

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.priority").isin("P2", "P3")), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.sum("ticket_requester_id").over(w_event_type_actor).alias("event_type_actor_sum_ticket_requester_id"),
        F.min("ticket_requester_id").over(w_all_by_ticket_closed_at_runn).alias("min_ticket_requester_id_so_far"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_actor_by_ticket_requester_id)).alias("days_since_prev_agent_hired_on"),
        F.count(F.lit(1)).over(w_actor_event_type).alias("rows_in_actor_event_type"),
        F.first("ticket_requester_id").over(w_all_by_agent_hired_on_full).alias("first_ticket_requester_id"),
        F.dense_rank().over(w_actor_by_ticket_requester_id).alias("ticket_requester_id_dense_rank"),
        F.max("ticket_requester_id").over(w_actor_by_ticket_requester_id_2).alias("max_ticket_requester_id_so_far"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_actor), 2).alias("ticket_requester_id_vs_event_type_actor_avg"),
        F.row_number().over(w_actor_by_ticket_requester_id).alias("seq_no"),
        F.max("ticket_requester_id").over(w_actor_event_type).alias("actor_event_type_max_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_actor_event_type_by_ticket_c).alias("ticket_requester_id_max_1_prior"),
        F.round(F.avg("ticket_requester_id").over(w_all_by_agent_hired_on_moving), 2).alias("ticket_requester_id_avg_3"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("seq_no_2"),
    )
)

annotated = (
    metrics
    .filter(F.col("ticket_requester_id_max_1_prior") <= 53)
    .withColumn("agent_name_ref", F.concat(F.col("agent_name"), F.lit("#"), F.col("seq_no").cast("string")))
)

with_trends = (
    annotated
    .select(
        "*",
        F.sum("ticket_requester_id_avg_3").over(w_event_type).alias("event_type_sum_ticket_requester_id_avg_3"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.ntile(10).over(w_ticket_id_seq_no_2_by_ticket).alias("ticket_closed_at_decile"),
        F.max("seq_no").over(w_ticket_id_ticket_closed_at_t).alias("ticket_id_ticket_closed_at_to_date_max_seq_no"),
        F.nth_value("seq_no_2", 2).over(w_all_by_ticket_requester_id_m).alias("second_seq_no_2"),
        F.round(F.percent_rank().over(w_all_by_ticket_requester_id_m_2), 3).alias("ticket_requester_id_max_1_prior_percent_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("agent_name_ref").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_closed_at",
        "ticket_requester_id",
        "agent_hired_on",
        "agent_name",
        "event_type_actor_sum_ticket_requester_id",
        "min_ticket_requester_id_so_far",
        "days_since_prev_agent_hired_on",
        "rows_in_actor_event_type",
        "first_ticket_requester_id",
        "max_ticket_requester_id_so_far",
        "ticket_requester_id_vs_event_type_actor_avg",
        "seq_no",
        "actor_event_type_max_ticket_requester_id",
        "ticket_requester_id_avg_3",
        "seq_no_2",
        "agent_name_ref",
        "ticket_closed_at_decile",
        "ticket_id_ticket_closed_at_to_date_max_seq_no",
        "second_seq_no_2",
        "ticket_requester_id_max_1_prior_percent_rank",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
    .orderBy(F.col("agent_name").desc(), F.col("actor").desc_nulls_first())
)
