from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

derived = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

enriched_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    enriched_ticket
    .filter(~F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_or_unknown",
        "actor_clean",
        "ticket_requester_id",
        "ticket_opened_at",
    )
    .orderBy(
        F.col("actor_clean").desc(),
        F.col("actor_or_unknown").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
