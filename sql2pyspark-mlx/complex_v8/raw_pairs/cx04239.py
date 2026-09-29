from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

screened = (
    with_details_ticket
    .crossJoin(with_details_ticket.agg(F.avg("ticket_requester_id").alias("overall_avg_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_avg_ticket_requester_id"))
    .drop("overall_avg_ticket_requester_id")
)

annotated = (
    screened
    .filter(F.lower(F.col("agent_name")).like("%ov%"))
    .select(
        "*",
        F.coalesce(F.col("ticket_requester_id"), F.lit(0)).alias("ticket_requester_id_filled"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.trunc(F.col("agent_hired_on"), "MM").alias("agent_hired_on_month_start"),
    )
)

result = (
    annotated
    .groupBy("ticket_id", "actor")
    .pivot("event_type", ["created", "escalated"])
    .agg(F.min("ticket_requester_id"))
)
