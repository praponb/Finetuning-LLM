from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("channel") == "email") & (F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)

priority_stats = (
    recent
    .groupBy("priority")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    recent.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

without_matches = (
    outliers.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority") == "P1") & (F.col("t.channel") == "web"), "left_semi")
    .filter(F.col("opened_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
)

with_children_events = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

flattened = (
    with_children_events
    .select(
        "id",
        "agent_id",
        "opened_at",
        "event_id",
        "event_actor",
        "event_at",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

result = (
    flattened
    .select(
        "id",
        "opened_at",
        "event_id",
        "event_actor",
        "event_at",
        "tag_pos",
        "tag",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.lower(F.regexp_replace(F.trim(F.col("tag")), " ", "_")).alias("tag_slug"),
    )
    .orderBy(F.col("opened_at").desc_nulls_first())
)
