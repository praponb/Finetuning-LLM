from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("requester_id").between(7, 25)) & (F.hour(F.col("closed_at")).between(10, 15)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

line_level = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

scored = (
    line_level
    .select(
        "*",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "event_id",
        "event_actor",
        "closed_at_label",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("closed_at_hour"))) / 60, 1).alias("minutes_closed_at_hour_to_event_at"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label_2"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "event_id",
        "event_actor",
        "closed_at_label",
        "closed_at_label_2",
        F.lower(F.trim(F.col("closed_at_label_2"))).alias("closed_at_label_2_clean"),
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
    )
    .orderBy(
        F.col("event_actor").desc_nulls_first(),
        F.col("closed_at_week").asc_nulls_last(),
        "id",
        "event_id",
    )
    .limit(20)
)
