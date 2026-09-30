from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 13)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P4"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_slug",
        "s.event_at_date",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.tier").alias("agent_tier"),
    )
)

per_label = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_slug",
        "event_at_date",
        "ticket_requester_id",
        "agent_tier",
        F.explode_outer("ticket_tags").alias("ticket_tag"),
    )
)

without_matches = (
    per_label
    .crossJoin(per_label.agg(F.min("ticket_requester_id").alias("overall_min_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_min_ticket_requester_id"))
    .drop("overall_min_ticket_requester_id")
)

filtered = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("reopened", "resolved", "created")), "left_semi")
    .filter(F.col("event_at_date").isNotNull())
)

joined = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id_2"),
    )
)

result = (
    joined
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_slug",
        "event_at_date",
        "ticket_requester_id",
        "ticket_tags",
        "ticket_requester_id_2",
        F.array_contains(F.col("ticket_tags"), "refund").alias("has_refund"),
        F.date_add(F.col("event_at_date"), 7).alias("event_at_date_plus_7d"),
    )
    .orderBy("event_at_date", F.col("ticket_requester_id").desc())
)
