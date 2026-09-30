from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_event_id_by_opened_a = Window.partitionBy("channel", "event_id").orderBy("opened_at")
w_channel_event_id_by_opened_a_2 = Window.partitionBy("channel", "event_id").orderBy("opened_at", "id", "event_id")

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

with_children_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.channel_prefix",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

sequenced = (
    with_children_events
    .select(
        "*",
        F.round(F.percent_rank().over(w_channel_event_id_by_opened_a), 3).alias("opened_at_percent_rank"),
        F.rank().over(w_channel_event_id_by_opened_a_2).alias("opened_at_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("event_type") == "created", 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.channel_prefix",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        "s.event_at",
        "s.opened_at_percent_rank",
        "s.opened_at_rank",
        "s.first_event_at",
        "s.event_event_type_count",
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

joined = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        "s.opened_at_percent_rank",
        "s.opened_at_rank",
        "s.first_event_at",
        "s.event_event_type_count",
        F.col("t.event_type").alias("event_event_type_2"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        "s.opened_at_percent_rank",
        "s.opened_at_rank",
        "s.first_event_at",
        "s.event_event_type_count",
        "s.event_event_type_2",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_details
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "channel",
        "event_id",
        "event_type",
        "event_actor",
        "opened_at_percent_rank",
        "opened_at_rank",
        "event_event_type_2",
        "agent_name",
        "agent_tier",
        "agent_hired_on",
        F.concat_ws(" / ", F.col("agent_name"), F.col("event_actor")).alias("agent_name_event_actor_label"),
    )
    .orderBy(F.col("event_actor").desc())
)
