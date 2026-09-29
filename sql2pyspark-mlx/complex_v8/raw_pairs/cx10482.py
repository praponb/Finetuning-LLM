from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_agent_id = Window.partitionBy("ticket_id", "ticket_agent_id")
w_event_type_by_agent_hired_on = Window.partitionBy("event_type").orderBy("agent_hired_on", "id")
w_actor = Window.partitionBy("actor")
w_agent_hired_on_month_by_tick = Window.partitionBy(F.month("agent_hired_on")).orderBy("ticket_closed_at")
w_agent_hired_on_month_by_tick_2 = Window.partitionBy(F.month("agent_hired_on")).orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_month_by_tick_3 = Window.partitionBy(F.month("agent_hired_on")).orderBy("ticket_closed_at", "id")
w_ticket_id_by_ticket_closed_a = Window.partitionBy("ticket_id").orderBy("ticket_closed_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_agent_id_event_type_b = Window.partitionBy("ticket_agent_id", "event_type").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_details_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_slug",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    with_details_ticket
    .select(
        "id",
        "event_at",
        "actor",
        "ticket_closed_at",
        "ticket_agent_id",
        "ticket_requester_id",
        "agent_hired_on",
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_id_ticket_agent_id), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_id_ticket_ag"),
        F.lag("agent_hired_on", 2).over(w_event_type_by_agent_hired_on).alias("prev2_agent_hired_on"),
        F.size(F.collect_set("event_type").over(w_ticket_id_ticket_agent_id)).alias("distinct_event_type_in_ticket_id_ticket_agent_id"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_actor), F.lit(0)), 4).alias("ticket_requester_id_share_of_actor"),
        F.round(F.cume_dist().over(w_agent_hired_on_month_by_tick), 3).alias("ticket_closed_at_cume_dist"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_actor), 2).alias("ticket_requester_id_vs_actor_avg"),
        F.first("event_at", ignorenulls=True).over(w_agent_hired_on_month_by_tick_2).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_agent_hired_on_month_by_tick_3), 3).alias("ticket_closed_at_cume_dist_2"),
        F.max("ticket_requester_id").over(w_ticket_id_by_ticket_closed_a).alias("ticket_requester_id_max_7"),
        F.nth_value("event_at", 2).over(w_ticket_agent_id_event_type_b).alias("second_event_at"),
    )
)

enriched = metrics.select("*", F.to_date(F.col("second_event_at")).alias("second_event_at_date"))

result = (
    enriched
    .select(
        "id",
        "ticket_closed_at",
        "agent_hired_on",
        "ticket_requester_id_share_of_ticket_id_ticket_ag",
        "prev2_agent_hired_on",
        "distinct_event_type_in_ticket_id_ticket_agent_id",
        "ticket_requester_id_share_of_actor",
        "ticket_closed_at_cume_dist",
        "ticket_requester_id_vs_actor_avg",
        "first_known_event_at",
        "ticket_closed_at_cume_dist_2",
        "second_event_at",
        "second_event_at_date",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.coalesce(F.col("ticket_requester_id"), F.lit(0)).alias("ticket_requester_id_filled"),
    )
)
