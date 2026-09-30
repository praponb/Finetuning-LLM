from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

annotated = (
    matched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.when(F.col("event_type").isin("reopened", "assigned"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.month(F.col("event_at")).alias("event_at_month"),
        F.round(F.col("hours_since_open") * 1.5, 2).alias("hours_since_open_adj"),
    )
)

enriched = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_group",
        "event_at_month",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type_group")), " ", "_")).alias("event_type_group_slug"),
        F.when(F.col("event_at_month") >= 9, "A").when(F.col("event_at_month") >= 6, "B").otherwise("C").alias("event_at_month_tier"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_group",
        "event_at_month",
        "event_at_label",
        "event_at_month_tier",
    )
    .orderBy(F.col("event_type_group").desc(), F.col("event_at_month").desc())
)
