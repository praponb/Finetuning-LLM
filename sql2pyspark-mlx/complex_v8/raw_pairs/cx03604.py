from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 18)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

labeled = (
    recent
    .filter(F.col("ticket_id") <= 21)
    .select(
        "*",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

enriched = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(7, 18)), "inner")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

scored = enriched.select("*", F.month(F.col("event_at")).alias("event_at_month"))

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_type_prefix",
        "event_at_month",
        F.when(F.col("ticket_priority").isin("P3", "P4"), "group_a").when(F.col("ticket_priority") == "P1", "group_b").otherwise("other").alias("ticket_priority_group"),
        F.abs(F.col("event_at_month") - 6).alias("event_at_month_distance"),
    )
    .orderBy(F.col("actor_event_type_label").desc_nulls_first(), "id")
    .limit(5)
)
