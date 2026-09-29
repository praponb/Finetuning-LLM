from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor = Window.partitionBy("event_actor")
w_channel_event_id_by_agent_hi = Window.partitionBy("channel", "event_id").orderBy(F.col("agent_hired_on").desc(), "id", "event_id")
w_channel_event_id_by_agent_hi_2 = Window.partitionBy("channel", "event_id").orderBy(F.col("agent_hired_on").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_event_id_by_agent_hi_3 = Window.partitionBy("channel", "event_id").orderBy(F.col("agent_hired_on").desc(), "id", "event_id").rowsBetween(-3, -1)

cleaned = (
    df_tickets
    .filter((F.col("priority") == "P4") & (F.col("csat") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.when(F.col("channel").isin("phone", "email"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "escalated", 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
    )
)

with_children_events = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.requester_id_distance",
        "s.event_event_type_count",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

with_parent = (
    with_children_events.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.requester_id_distance",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "agent_id",
        "channel",
        "event_id",
        "event_type",
        "agent_team",
        "agent_hired_on",
        F.round(F.col("requester_id_distance") - F.avg("requester_id_distance").over(w_event_actor), 2).alias("requester_id_distance_vs_event_actor_avg"),
        F.count(F.lit(1)).over(w_event_actor).alias("rows_in_event_actor"),
        F.round(F.cume_dist().over(w_channel_event_id_by_agent_hi), 3).alias("agent_hired_on_cume_dist"),
        F.sum("requester_id_distance").over(w_channel_event_id_by_agent_hi_2).alias("running_requester_id_distance"),
        F.sum("requester_id_distance").over(w_event_actor).alias("event_actor_sum_requester_id_distance"),
        F.round(F.avg("requester_id_distance").over(w_channel_event_id_by_agent_hi_3), 2).alias("requester_id_distance_avg_3_prior"),
    )
)

combined_details_event = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.event_id",
        "s.agent_team",
        "s.agent_hired_on",
        "s.rows_in_event_actor",
        "s.agent_hired_on_cume_dist",
        "s.running_requester_id_distance",
        "s.event_actor_sum_requester_id_distance",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

enriched = (
    combined_details_event.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.event_ticket_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.event_id",
        "s.agent_team",
        "s.agent_hired_on",
        "s.running_requester_id_distance",
        "s.event_actor_sum_requester_id_distance",
        "s.event_ticket_id",
        "s.event_actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_id",
        "event_actor_sum_requester_id_distance",
        "running_requester_id_distance",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["event_actor_sum_requester_id_distance", "running_requester_id_distance"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("id", "event_id", "field")
)
