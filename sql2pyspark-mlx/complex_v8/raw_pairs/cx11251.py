from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

derived = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_opened_at",
        "ticket_agent_id",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

result = (
    derived
    .groupingSets(
        [["actor_len", "ticket_id", "ticket_agent_id"], ["actor_len"], ["ticket_id"], []],
        "actor_len", "ticket_id", "ticket_agent_id",
    )
    .agg(
        F.round(F.sum("event_at_dayofweek") / F.nullif(F.sum("actor_len"), F.lit(0)), 3).alias("event_at_dayofweek_to_actor_len_ratio"),
        F.max("ticket_opened_at").alias("last_ticket_opened_at"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .orderBy(
        F.col("actor_len").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_agent_id").asc_nulls_last(),
    )
)
