from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")
w_all_by_ticket_requester_id = Window.orderBy("ticket_requester_id", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_prior = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(-3, -1)

valid_events = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

enriched_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") > 7)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_date",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_date",
        "ticket_priority",
        "agent_hired_on",
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.size(F.collect_set("ticket_priority").over(w_actor)).alias("distinct_ticket_priority_in_actor"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_ticket_requester_id)).alias("days_since_prev_agent_hired_on"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.last("event_at", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("last_known_event_at"),
        F.round(F.avg("ticket_requester_id").over(w_event_type_by_event_at_prior), 2).alias("ticket_requester_id_avg_3_prior"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_actor), 2).alias("ticket_requester_id_vs_actor_avg"),
        F.round(F.avg("ticket_requester_id").over(w_actor), 2).alias("actor_avg_ticket_requester_id"),
    )
)

leaders = (
    metrics
    .filter(F.col("event_at_dense_rank") <= 2)
    .drop("event_at_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "event_at",
        "event_at_date",
        "agent_hired_on",
        "rows_in_actor",
        "distinct_ticket_priority_in_actor",
        "days_since_prev_agent_hired_on",
        "last_known_event_at",
        "ticket_requester_id_avg_3_prior",
        "ticket_requester_id_vs_actor_avg",
        "actor_avg_ticket_requester_id",
        F.abs(F.col("rows_in_actor") - 13).alias("rows_in_actor_distance"),
    )
    .orderBy("rows_in_actor")
)
