from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") != "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "login")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_hour",
        "s.actor_group",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    enriched_2
    .groupingSets(
        [["actor_group", "ticket_csat"], ["actor_group"]],
        "actor_group", "ticket_csat",
    )
    .agg(F.round(F.stddev("ticket_csat"), 2).alias("stddev_ticket_csat"))
    .select(
        F.coalesce(F.col("actor_group"), F.lit("ALL")).alias("actor_group"),
        "ticket_csat",
        "stddev_ticket_csat",
    )
)
