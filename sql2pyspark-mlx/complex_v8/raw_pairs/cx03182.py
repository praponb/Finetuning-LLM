from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_team = Window.partitionBy("agent_id", "agent_team")
w_opened_at_year_by_agent_hire = Window.partitionBy(F.year("opened_at")).orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("opened_at") < F.lit("2025-02-18 14:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
    )
)

enriched = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "agent_team",
        "agent_hired_on",
        "agent_name",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_agent_team), 2).alias("requester_id_vs_agent_id_agent_team_avg"),
        F.nth_value("agent_hired_on", 2).over(w_opened_at_year_by_agent_hire).alias("second_agent_hired_on"),
        F.nth_value("requester_id", 2).over(w_opened_at_year_by_agent_hire).alias("second_requester_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.first_event_at",
    )
)

with_children_events = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") != "resolved"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.agent_team",
        "s.agent_hired_on",
        "s.agent_name",
        "s.requester_id_vs_agent_id_agent_team_avg",
        "s.second_requester_id",
        "s.event_count",
        "s.first_event_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children_events
    .groupBy("agent_team", "event_actor")
    .pivot("event_type", ["created", "replied", "assigned"])
    .agg(F.max("second_requester_id"))
)
