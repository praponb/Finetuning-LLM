from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

detail_rows_tickets = (
    matched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel").isNotNull()), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

derived = (
    detail_rows_tickets
    .select(
        "id",
        "priority",
        "ticket_id_2",
        "ticket_opened_at",
        "ticket_channel",
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("ticket_id_2").cast("string")).alias("ticket_channel_ref"),
        F.concat_ws("-", F.col("ticket_channel"), F.col("priority")).alias("ticket_channel_priority_label"),
    )
)

result = (
    derived
    .filter(F.col("ticket_id_2") <= 91)
    .select(
        "id",
        "ticket_id_2",
        "ticket_opened_at",
        "ticket_channel",
        "ticket_channel_ref",
        "ticket_channel_priority_label",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel_ref")), " ", "_")).alias("ticket_channel_ref_slug"),
        F.when(F.col("priority").isin("P3", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)
