from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

joined_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

annotated = (
    joined_agent
    .select(
        "id",
        "opened_at",
        "csat",
        "requester_id_plus_csat",
        "agent_tier",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.abs(F.col("requester_id_plus_csat") - 10).alias("requester_id_plus_csat_distance"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

summarized_rows = (
    annotated.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "opened_at",
        "csat",
        "agent_tier",
        "requester_id_filled",
        "requester_id_plus_csat_distance",
        "first_event_at",
        F.substring(F.trim(F.col("agent_tier")), 1, 3).alias("agent_tier_prefix"),
        F.to_date(F.col("first_event_at")).alias("first_event_at_date"),
    )
    .orderBy("id", F.col("first_event_at").asc())
)
