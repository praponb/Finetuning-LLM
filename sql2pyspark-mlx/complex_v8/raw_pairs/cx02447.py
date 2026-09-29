from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "escalated"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

enriched = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.event_at_day",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.concat_ws(": ", F.col("event_type"), F.col("event_type_group")).alias("event_type_event_type_group_label"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat(F.col("event_type_group"), F.lit("#"), F.col("id").cast("string")).alias("event_type_group_ref"),
    )
)

labeled_2 = (
    labeled
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_group",
        "event_at_day",
        "ticket_opened_at",
        "event_type_event_type_group_label",
        "actor_slug",
        F.month(F.col("event_at_day")).alias("event_at_day_month"),
    )
)

annotated = (
    labeled_2
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_day",
        "event_type_event_type_group_label",
        "actor_slug",
        F.coalesce(F.col("event_type_group"), F.lit("UNKNOWN")).alias("event_type_group_or_unknown"),
    )
)

enriched_2 = (
    annotated
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_day",
        "actor_slug",
        "event_type_group_or_unknown",
        F.quarter(F.col("event_at_day")).alias("event_at_day_quarter"),
        F.quarter(F.col("event_at_day")).alias("event_at_day_quarter_2"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "actor",
        "event_at_day",
        "actor_slug",
        "event_at_day_quarter",
        "event_at_day_quarter_2",
        F.concat_ws("|", F.col("actor_slug"), F.col("actor")).alias("actor_slug_actor_label"),
    )
)
