from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 10) & (F.hour(F.col("event_at")).between(10, 16)))
)

joined = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_parent_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

qualified = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.col("t.channel").isin("chat", "phone", "email")), "left_anti")
)

derived = (
    qualified
    .filter(F.col("event_type") == "created")
    .select(
        "*",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.when(F.col("ticket_requester_id") >= 15, "HIGH").when(F.col("ticket_requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("ticket_requester_id_tier"),
    )
)

grouped = (
    derived
    .groupBy("actor", "ticket_agent_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("ticket_requester_id").alias("max_ticket_requester_id"),
        F.countDistinct("ticket_requester_id_tier").alias("distinct_ticket_requester_id_tiers"),
    )
)

result = (
    grouped
    .filter(F.col("max_ticket_requester_id") >= 11)
    .select(
        "actor",
        "cnt",
        "max_ticket_requester_id",
        "distinct_ticket_requester_id_tiers",
    )
    .orderBy("distinct_ticket_requester_id_tiers", F.col("actor").desc_nulls_first())
)
