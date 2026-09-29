from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_hour_event_type = Window.partitionBy("event_at_hour", "event_type")
w_agent_team_by_ticket_csat_ru = Window.partitionBy("agent_team").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_by_ticket_csat_fu = Window.partitionBy("agent_team").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

enriched_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(~F.array_contains(F.col("t.tags"), "upgrade"))
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

derived = (
    enriched_ticket
    .select(
        "*",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.size(F.col("ticket_tags")).alias("ticket_tags_count"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("t.event_type").isNotNull()), "left_semi")
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.days_since_event_at",
        "s.ticket_tags",
        "s.agent_name",
        "s.event_at_hour",
        "s.ticket_tags_count",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_type",
        "days_since_event_at",
        "agent_name",
        "event_at_hour",
        "ticket_csat",
        "agent_hired_on",
        "agent_team",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_at_hour_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_event_at_hour_event"),
        F.count(F.lit(1)).over(w_agent_team_by_ticket_csat_ru).alias("running_count"),
        F.nth_value("agent_hired_on", 2).over(w_agent_team_by_ticket_csat_fu).alias("second_agent_hired_on"),
        F.nth_value("ticket_tags_count", 2).over(w_agent_team_by_ticket_csat_fu).alias("second_ticket_tags_count"),
        F.size(F.collect_set("agent_team").over(w_event_at_hour_event_type)).alias("distinct_agent_team_in_event_at_hour_event_type"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("ticket_csat").alias("m_ticket_csat"),
        F.col("distinct_agent_team_in_event_at_hour_event_type").alias("m_distinct_agent_team_in_event_at_hour_event_typ"),
        F.col("days_since_event_at").alias("days_since_event_at_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_ticket_csat", "m_distinct_agent_team_in_event_at_hour_event_typ", "days_since_event_at_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .orderBy("id", "metric")
)
