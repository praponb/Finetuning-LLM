from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "ticket_id").orderBy("event_at_day", "id")

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") != "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at_2",
    )
)

latest = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_day",
        "s.event_at_label",
        "s.event_type_ref",
        "s.last_event_at",
        "s.last_event_at_2",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

with_parent_2 = (
    with_parent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_day",
        "s.event_at_label",
        "s.event_type_ref",
        "s.last_event_at",
        "s.last_event_at_2",
        "s.ticket_channel",
        "s.agent_name",
        F.col("t.channel").alias("ticket_channel_2"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team_2"),
    )
)

with_parent_3 = (
    with_parent_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.channel").isin("phone", "web", "chat"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_day",
        "s.event_at_label",
        "s.event_type_ref",
        "s.last_event_at",
        "s.last_event_at_2",
        "s.ticket_channel",
        "s.agent_name",
        "s.ticket_channel_2",
        "s.ticket_csat",
        "s.agent_team_2",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.channel").alias("ticket_channel_3"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.team").alias("agent_team"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") == "resolved", 1)).alias("event_event_type_count"),
    )
)

summarized_rows_2 = (
    with_parent_3.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    summarized_rows_2
    .filter(F.hour(F.col("last_event_at")).between(7, 12))
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_day",
        "last_event_at",
        "ticket_channel",
        "agent_name",
        "ticket_channel_2",
        "ticket_csat",
        "agent_team_2",
        "ticket_requester_id",
        "ticket_opened_at",
        "agent_team",
        "event_count",
        "event_event_type_count",
        F.abs(F.col("ticket_requester_id") - 18).alias("ticket_requester_id_distance"),
    )
    .orderBy(
        F.col("ticket_requester_id_distance").desc(),
        F.col("event_count").asc(),
    )
)
