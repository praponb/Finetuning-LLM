from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "agent")) | (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

combined_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.hour(F.col("t.opened_at")).between(7, 12))
    .select("s.*", F.col("t.opened_at").alias("ticket_opened_at"))
)

with_activity = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") > 5), "left_semi")
)

result = (
    with_activity
    .groupBy("event_type", "ticket_opened_at")
    .pivot("actor_group", ["group_a", "group_b"])
    .agg(F.count("event_at_hour"))
    .orderBy("event_type", "ticket_opened_at")
)
