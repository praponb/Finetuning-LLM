from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "agent_tier").orderBy(F.col("closed_at").desc(), F.col("id").desc())

base_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel_ref",
        "s.channel_priority_label",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

agent_id_stats = (
    with_details
    .groupBy("agent_id")
    .agg(F.max("requester_id").alias("max_requester_id"))
)

vs_group = (
    with_details.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") == F.col("g.max_requester_id"))
    .select(
        "s.*",
        "g.max_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.max_requester_id"), 2).alias("requester_id_minus_max"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_stats = (
    vs_group.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at", "t.first_event_at")
)

latest_per_key = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
    .filter(F.col("agent_tier").isin("T3", "T2"))
)

result = (
    with_activity
    .groupBy("last_event_at", "first_event_at")
    .pivot("priority", ["P4", "P1", "P2", "P3"])
    .agg(F.count("requester_id_minus_max"))
    .orderBy("last_event_at", "first_event_at")
)
