from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_agent_team = Window.partitionBy("ticket_csat", "agent_team")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

with_parent_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.days_since_event_at",
        "s.actor_event_type_label",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "days_since_event_at",
        "ticket_csat",
        "agent_team",
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_ticket_csat_agent_team), 2).alias("days_since_event_at_vs_ticket_csat_agent_team_av"),
        F.count(F.lit(1)).over(w_ticket_csat_agent_team).alias("rows_in_ticket_csat_agent_team"),
    )
)

result = (
    windowed
    .rollup("ticket_id", "ticket_csat")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("agent_team").isin("Tech", "Billing"), F.col("days_since_event_at")).otherwise(0)).alias("days_since_event_at_agent_team_not_in_tech_billi"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_csat").asc_nulls_last(),
    )
)
