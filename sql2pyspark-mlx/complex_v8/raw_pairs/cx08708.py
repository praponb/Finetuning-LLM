from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

tagged = (
    recent
    .filter(F.hour(F.col("event_at")).between(8, 13))
    .select(
        "*",
        F.when(F.col("actor_or_unknown").isin("customer", "UNKNOWN"), "group_a").when(F.col("actor_or_unknown") == "system", "group_b").otherwise("other").alias("actor_or_unknown_group"),
        F.concat_ws("-", F.col("actor_event_type_label"), F.col("event_type")).alias("actor_event_type_label_event_type_label"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

combined_details_ticket = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") < 18)
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "actor_or_unknown",
        "actor_event_type_label_event_type_label",
        "actor_clean",
        "ticket_requester_id",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
    )
    .orderBy(F.col("actor").asc(), F.col("actor_or_unknown").asc_nulls_last())
)
