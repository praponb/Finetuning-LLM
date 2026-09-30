from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("event_type") == "created") & (F.col("ticket_id") <= 54))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

combined_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "outage"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_len",
        "s.actor_slug",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

combined_details_ticket_2 = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_slug",
        "s.ticket_agent_id",
        "s.ticket_requester_id",
        "s.ticket_tags",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

per_group = (
    combined_details_ticket_2
    .groupBy(
        "event_type",
        F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"),
    )
    .agg(
        F.percentile_approx("ticket_requester_id", 0.5).alias("median_ticket_requester_id"),
        F.count("*").alias("cnt"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.min("ticket_requester_id").alias("min_ticket_requester_id"),
    )
)

labeled = (
    per_group
    .withColumn("event_type_group", F.when(F.col("event_type").isin("replied", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other"))
)

annotated = (
    labeled
    .select(
        "*",
        F.concat(F.col("event_type_group"), F.lit("#"), F.col("cnt").cast("string")).alias("event_type_group_ref"),
        F.length(F.trim(F.col("event_type_group"))).alias("event_type_group_len"),
        F.round(F.col("cnt") / F.nullif(F.col("min_ticket_requester_id"), F.lit(0)), 3).alias("cnt_per_min_ticket_requester_id"),
    )
)

result = (
    annotated
    .filter(F.col("event_type").isNotNull())
    .select(
        "event_type",
        "agent_hired_on_ym",
        "median_ticket_requester_id",
        "cnt",
        "distinct_ticket_ids",
        "min_ticket_requester_id",
        "event_type_group",
        "event_type_group_ref",
        "cnt_per_min_ticket_requester_id",
        (F.col("cnt") + F.col("distinct_ticket_ids")).alias("cnt_plus_distinct_ticket_ids"),
    )
    .orderBy(F.col("event_type").desc(), F.col("event_type_group").desc())
)
