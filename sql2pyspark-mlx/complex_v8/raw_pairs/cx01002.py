from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

with_children_events = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

joined = (
    with_children_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_month",
        "s.event_id",
        "s.event_event_type",
        "s.event_actor",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

line_level = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_id",
        "s.event_event_type",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.ticket_agent_id",
        "s.last_event_at",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_type").alias("event_event_type_2"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

tagged = (
    line_level
    .filter(F.col("event_event_type") == "created")
    .select(
        "id",
        "event_at",
        "event_type",
        "event_id",
        "event_event_type",
        "ticket_tags",
        "ticket_channel",
        "ticket_agent_id",
        "event_id_2",
        "event_actor_2",
        "event_event_at",
        F.when(F.col("event_actor_2").isin("agent", "system"), "group_a").when(F.col("event_actor_2") == "customer", "group_b").otherwise("other").alias("event_actor_2_group"),
        F.concat(F.col("event_event_type_2"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_event_type_2_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "event_event_type",
        "ticket_tags",
        "ticket_channel",
        "ticket_agent_id",
        "event_id_2",
        "event_actor_2",
        "event_event_type_2_ref",
        F.concat_ws(": ", F.col("event_actor_2_group"), F.col("event_actor_2")).alias("event_actor_2_group_event_actor_2_label"),
    )
    .orderBy("id", F.col("event_actor_2").asc_nulls_last())
)
