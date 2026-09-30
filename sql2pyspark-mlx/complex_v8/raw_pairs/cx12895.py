from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_team", "event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

with_parent_agent = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.agent_id",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

screened = (
    with_parent_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "reopened") & (F.col("t.event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_semi")
)

latest_per_key = (
    screened
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "agent_tier",
        "agent_team",
        F.when(F.col("agent_team").isin("Billing", "Onboarding"), "group_a").when(F.col("agent_team") == "VIP", "group_b").otherwise("other").alias("agent_team_group"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last())
)
