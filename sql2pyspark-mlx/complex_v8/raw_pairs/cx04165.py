from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "customer")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

combined_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_clean",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_parent = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-03-14").cast("date")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_clean",
        "s.ticket_channel",
        "s.ticket_agent_id",
        F.col("t.channel").alias("ticket_channel_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

totals = (
    with_parent
    .filter(F.col("event_type").isNotNull())
    .groupBy(
        "ticket_channel_2",
        "ticket_id",
        F.date_trunc("month", "ticket_opened_at").alias("ticket_opened_at_month"),
    )
    .agg(
        F.countDistinct("ticket_channel_2").alias("distinct_ticket_channel_2s"),
        F.round(100 * F.count(F.when(~F.col("ticket_channel_2").isin("email", "web"), 1)) / F.count("*"), 1).alias("pct_ticket_channel_2_not_in_emai"),
        F.countDistinct("ticket_channel").alias("distinct_ticket_channels"),
    )
)

result = (
    totals
    .select(
        "ticket_channel_2",
        "ticket_id",
        "ticket_opened_at_month",
        F.when(F.col("ticket_channel_2").isin("chat", "phone"), "group_a").when(F.col("ticket_channel_2") == "email", "group_b").otherwise("other").alias("ticket_channel_2_group"),
        F.length(F.trim(F.col("ticket_channel_2"))).alias("ticket_channel_2_len"),
    )
    .orderBy(
        F.col("ticket_channel_2_group").desc(),
        F.col("ticket_id").desc_nulls_first(),
    )
)
