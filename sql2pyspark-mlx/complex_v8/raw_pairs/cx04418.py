from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("ticket_id") <= 48))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

enriched = (
    scoped
    .filter(F.col("actor") == "agent")
    .select("*", F.length(F.trim(F.col("event_type"))).alias("event_type_len"))
)

joined = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_len",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    joined
    .groupingSets(
        [["event_type_len", "ticket_id", "ticket_priority"], ["event_type_len"], ["ticket_id"]],
        "event_type_len", "ticket_id", "ticket_priority",
    )
    .agg(
        F.min("event_type_len").alias("min_event_type_len"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("event_type_len").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_priority").asc_nulls_last(),
    )
)
