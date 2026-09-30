from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")
w_event_type_event_at_month_by = Window.partitionBy("event_type", F.month("event_at")).orderBy(F.col("ticket_requester_id").desc(), "id")
w_actor_by_ticket_opened_at = Window.partitionBy("actor").orderBy(F.col("ticket_opened_at").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_actor_agent_hired_on_year_by = Window.partitionBy("actor", F.year("agent_hired_on")).orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_opened_at_pr = Window.partitionBy("actor").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_ticket_events
    .filter((F.col("event_type").isin("replied", "escalated", "resolved")) | (F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P2"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_requester_id",
        "ticket_tags",
        "ticket_opened_at",
        F.size(F.collect_set("event_type").over(w_ticket_id_actor)).alias("distinct_event_type_in_ticket_id_actor"),
        F.min("ticket_requester_id").over(w_ticket_id_actor).alias("ticket_id_actor_min_ticket_requester_id"),
        F.ntile(10).over(w_event_type_event_at_month_by).alias("ticket_requester_id_decile"),
        F.row_number().over(w_actor_by_ticket_opened_at).alias("rn"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_id), 2).alias("ticket_requester_id_vs_ticket_id_avg"),
        F.sum("ticket_requester_id").over(w_actor_agent_hired_on_year_by).alias("running_ticket_requester_id"),
        F.min("ticket_requester_id").over(w_ticket_id).alias("ticket_id_min_ticket_requester_id"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_id_actor), 2).alias("ticket_id_actor_avg_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_actor_by_ticket_opened_at_pr).alias("ticket_requester_id_max_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("rn").between(3, 7))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_requester_id",
        "ticket_tags",
        "ticket_opened_at",
        "distinct_event_type_in_ticket_id_actor",
        "ticket_id_actor_min_ticket_requester_id",
        "ticket_requester_id_decile",
        "rn",
        "ticket_requester_id_vs_ticket_id_avg",
        "running_ticket_requester_id",
        "ticket_id_min_ticket_requester_id",
        "ticket_id_actor_avg_ticket_requester_id",
        "ticket_requester_id_max_3_prior",
        (F.floor(F.col("ticket_requester_id") / 5) * 5).alias("ticket_requester_id_bucket"),
    )
    .orderBy(F.col("distinct_event_type_in_ticket_id_actor").desc_nulls_first())
)
