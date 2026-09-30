from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("channel").isin("email", "web")) & (F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
    )
)

without_matches = (
    filtered_tickets
    .crossJoin(filtered_tickets.agg(F.max("csat").alias("overall_max_csat")))
    .filter(F.col("csat") < F.col("overall_max_csat"))
    .drop("overall_max_csat")
)

with_children = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.channel_slug",
        "s.channel_ref",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

with_parent = (
    with_children.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel_slug",
        "s.channel_ref",
        "s.event_id",
        "s.event_type",
        "s.event_at",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("t.actor").alias("event_actor"),
    )
)

annotated = (
    with_parent
    .filter(F.col("agent_id").isNotNull())
    .withColumn("agent_hired_on_month_start", F.trunc(F.col("agent_hired_on"), "MM"))
)

result = (
    annotated
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel_slug",
        "channel_ref",
        "event_id",
        "event_type",
        "event_at",
        "agent_team",
        "agent_hired_on",
        "event_actor",
        "agent_hired_on_month_start",
        F.trunc(F.col("agent_hired_on"), "MM").alias("agent_hired_on_month_start_2"),
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
    .orderBy(F.col("agent_id").desc_nulls_first(), "agent_team_or_unknown")
)
