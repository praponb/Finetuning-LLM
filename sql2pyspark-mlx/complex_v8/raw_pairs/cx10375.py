from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(2, 3)) & (F.col("channel").isin("email", "chat")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
    )
)

enriched = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "closed_at_label",
        "least_csat_requester_id",
        "closed_at_hour",
        F.concat_ws(" / ", F.col("channel"), F.col("closed_at_label")).alias("channel_closed_at_label_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") == "created", 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count_2"),
    )
)

profiled = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("resolved", "reopened", "assigned")), "left_semi")
    .filter(F.col("event_count_2") <= 7)
)

result = (
    filtered
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "closed_at_label",
        "least_csat_requester_id",
        "closed_at_hour",
        "channel_closed_at_label_label",
        "event_count",
        "event_event_type_count",
        "event_count_2",
    )
    .orderBy(F.col("least_csat_requester_id").asc())
)
