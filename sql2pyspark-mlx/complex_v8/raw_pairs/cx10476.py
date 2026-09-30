from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 53) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

derived = filtered_events.select("*", F.upper(F.trim(F.col("actor"))).alias("actor_clean"))

qualified = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system") & (F.col("t.event_at") < F.lit("2025-03-02 14:00:00").cast("timestamp")), "left_anti")
    .filter(F.hour(F.col("event_at")).between(6, 11))
)

with_details_ticket = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_label",
        "s.actor_clean",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    with_details_ticket.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

derived_2 = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_clean",
        "ticket_channel",
        "agent_hired_on",
        "first_event_at_2",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

annotated = (
    derived_2
    .select(
        "*",
        F.substring(F.trim(F.col("actor_clean")), 1, 3).alias("actor_clean_prefix"),
        F.when(F.col("ticket_channel").isin("chat", "web"), "group_a").when(F.col("ticket_channel") == "phone", "group_b").otherwise("other").alias("ticket_channel_group"),
    )
)

result = (
    annotated
    .filter(F.col("agent_hired_on") >= F.lit("2024-05-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_clean",
        "ticket_channel",
        "first_event_at_2",
        "event_at_date",
        "actor_clean_prefix",
        "ticket_channel_group",
        F.initcap(F.trim(F.col("actor_clean"))).alias("actor_clean_clean"),
        F.datediff(F.col("agent_hired_on"), F.col("event_at_date")).alias("days_event_at_date_to_agent_hired_on"),
    )
    .orderBy("event_at_date", F.col("actor_clean_clean").desc())
)
