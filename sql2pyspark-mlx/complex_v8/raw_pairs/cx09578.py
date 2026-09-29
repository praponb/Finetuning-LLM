from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

annotated = (
    with_parent
    .filter(F.col("actor") == "system")
    .select(
        "*",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

result = (
    annotated
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_closed_at",
        "actor_len",
        "event_at_label",
    )
    .orderBy(F.col("event_at_label").desc_nulls_first(), "id")
    .limit(10)
)
