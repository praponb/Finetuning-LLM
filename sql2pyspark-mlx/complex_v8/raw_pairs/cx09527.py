from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_agent_team = Window.partitionBy("ticket_channel", "agent_team")
w_event_type_ticket_channel_by = Window.partitionBy("event_type", "ticket_channel").orderBy("ticket_closed_at", "id")
w_all_by_ticket_csat_running = Window.orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")

prepared_events = df_ticket_events

with_details_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_channel",
        "ticket_closed_at",
        "agent_team",
        F.max("ticket_csat").over(w_ticket_channel_agent_team).alias("ticket_channel_agent_team_max_ticket_csat"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_ticket_channel_by))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_ticket_csat_running).alias("last_known_event_at"),
        F.round(F.avg("ticket_csat").over(w_ticket_channel_agent_team), 2).alias("ticket_channel_agent_team_avg_ticket_csat"),
        F.size(F.collect_set("ticket_id").over(w_event_type)).alias("distinct_ticket_id_in_event_type"),
        F.size(F.collect_set("actor").over(w_event_type)).alias("distinct_actor_in_event_type"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.min("ticket_csat").over(w_event_type).alias("event_type_min_ticket_csat"),
        F.lead("ticket_csat", 1, 0).over(w_event_type_ticket_channel_by).alias("next_ticket_csat"),
        F.count(F.lit(1)).over(w_ticket_channel_agent_team).alias("rows_in_ticket_channel_agent_team"),
        F.lag("event_at").over(w_all_by_ticket_closed_at).alias("prev_event_at"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.sum("ticket_csat").over(w_ticket_channel_agent_team).alias("ticket_channel_agent_team_sum_ticket_csat"),
    )
)

result = (
    sequenced
    .filter(F.col("event_type") != "created")
    .select(
        "id",
        "event_type",
        "actor",
        "ticket_channel",
        "ticket_closed_at",
        "ticket_channel_agent_team_max_ticket_csat",
        "minutes_since_prev_event_at",
        "last_known_event_at",
        "ticket_channel_agent_team_avg_ticket_csat",
        "distinct_ticket_id_in_event_type",
        "distinct_actor_in_event_type",
        "rows_in_event_type",
        "event_type_min_ticket_csat",
        "rows_in_ticket_channel_agent_team",
        "prev_event_at",
        "ticket_channel_agent_team_sum_ticket_csat",
        F.to_date(F.col("ticket_closed_at")).alias("ticket_closed_at_date"),
        F.abs(F.col("event_at_quintile") - 4).alias("event_at_quintile_distance"),
    )
    .orderBy(
        F.col("id").desc(),
        F.col("distinct_ticket_id_in_event_type").asc_nulls_last(),
    )
)
