from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 3))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

combined_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-18").cast("date")), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.event_type_len",
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

derived = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_or_unknown",
        "agent_name",
        F.substring(F.trim(F.col("agent_name")), 1, 4).alias("agent_name_prefix"),
    )
)

scored = (
    derived
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_or_unknown",
        "agent_name",
        F.concat(F.col("actor_or_unknown"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_or_unknown_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_name_prefix")), " ", "_")).alias("agent_name_prefix_slug"),
    )
)

tagged = (
    scored
    .select(
        "*",
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.coalesce(F.col("actor_or_unknown"), F.lit("UNKNOWN")).alias("actor_or_unknown_or_unknown"),
    )
)

qualified = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") == "email"), "left_anti")
)

with_parent = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.agent_name",
        "s.agent_name_prefix_slug",
        "s.actor_group",
        "s.actor_or_unknown_or_unknown",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    with_parent
    .filter(F.length(F.trim(F.col("agent_name"))) >= 6)
    .select(
        "id",
        "actor",
        "actor_or_unknown",
        "agent_name_prefix_slug",
        "actor_group",
        "actor_or_unknown_or_unknown",
        "ticket_tags",
        "ticket_csat",
        "ticket_opened_at",
        F.size(F.col("ticket_tags")).alias("ticket_tags_count"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("actor_or_unknown_or_unknown").asc(),
    )
)
