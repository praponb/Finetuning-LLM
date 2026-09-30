from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_parent = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T3"), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

derived = (
    with_parent
    .filter(F.col("csat") < 5)
    .select(
        "*",
        F.size(F.col("tags")).alias("tags_count"),
        F.date_trunc("year", F.col("last_event_at")).alias("last_event_at_year"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier_2"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "channel_len",
        "channel_clean",
        "first_event_at",
        "event_count",
        "last_event_at_year",
        "agent_tier_2",
        "agent_name",
        "agent_team",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("last_event_at_year"))) / 60, 1).alias("minutes_last_event_at_year_to_closed_at"),
    )
    .orderBy(F.col("opened_at").desc())
)
