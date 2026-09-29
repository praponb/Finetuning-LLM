from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_tier").orderBy("opened_at", "id")

cleaned_tickets = df_tickets.filter(F.hour(F.col("closed_at")).between(9, 13))

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("event_type").isin("reopened", "created"), 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    cleaned_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

labeled = (
    with_stats
    .filter(F.col("csat").between(1, 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "event_event_type_count",
        "event_count",
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

enriched_agent = (
    labeled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.tags",
        "s.event_count",
        "s.opened_at_label",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

latest = (
    enriched_agent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_agent = (
    latest.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") != "Tech")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.event_count",
        "s.opened_at_label",
        "s.agent_name",
        "s.agent_team",
        "s.agent_tier",
        F.col("su.team").alias("agent_team_2"),
    )
)

result = (
    joined_agent
    .select(
        "id",
        "agent_id",
        "csat",
        "event_count",
        "opened_at_label",
        "agent_name",
        "agent_team",
        "agent_tier",
        "agent_team_2",
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
    )
    .orderBy("agent_tier", "id")
    .limit(20)
)
