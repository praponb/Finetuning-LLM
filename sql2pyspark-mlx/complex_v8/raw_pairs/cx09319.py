from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("channel").isin("web", "chat", "email")) & (F.col("csat") >= 2))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.least(F.col("requester_id"), F.col("csat")).alias("least_requester_id_csat"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
    )
)

scored = (
    base_tickets
    .filter(~F.col("channel").isin("email", "phone"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "priority_or_unknown",
        "least_requester_id_csat",
        "hours_closed_at_to_opened_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

scored_2 = (
    scored
    .withColumn("least_days_since_closed_at_least_requester_id_cs", F.least(F.col("days_since_closed_at"), F.col("least_requester_id_csat")))
)

qualified = (
    scored_2.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") <= 14) & (F.col("t.channel") != "phone"), "left_semi")
)

without_matches = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("assigned", "resolved", "replied")) & (F.col("t.event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")), "left_anti")
)

result = (
    without_matches
    .filter(F.col("days_since_closed_at") > 280)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "priority_or_unknown",
        "least_requester_id_csat",
        "hours_closed_at_to_opened_at",
        "days_since_closed_at",
        "least_days_since_closed_at_least_requester_id_cs",
        F.concat_ws("-", F.col("channel"), F.col("priority_or_unknown")).alias("channel_priority_or_unknown_label"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
    .orderBy("channel_priority_or_unknown_label")
)
