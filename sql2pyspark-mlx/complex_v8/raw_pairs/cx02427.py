from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("event_type") != "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_details_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_slug",
        "s.event_type_actor_label",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_details_ticket
    .cube("actor", "ticket_agent_id", "ticket_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
    )
)
