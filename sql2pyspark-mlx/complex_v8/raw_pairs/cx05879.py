from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_event_id_by_agent_h = Window.partitionBy("agent_id", "event_id").orderBy(F.col("agent_hired_on").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_event_id_by_agent_h_2 = Window.partitionBy("agent_id", "event_id").orderBy(F.col("agent_hired_on").desc(), "id", "event_id")

scoped = (
    df_tickets
    .filter((F.col("priority").isin("P2", "P3", "P4")) | (F.col("requester_id") < 23))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

with_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.has_bug",
        "s.channel_clean",
        "s.channel_ref",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

screened = (
    with_details_agent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

joined_agent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.channel_clean",
        "s.channel_ref",
        "s.agent_team",
        "s.agent_hired_on",
        F.col("su.team").alias("agent_team_2"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier_2"),
    )
)

exploded_detail_events = (
    joined_agent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.csat",
        "s.channel_clean",
        "s.channel_ref",
        "s.agent_hired_on",
        "s.agent_team_2",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

metrics = (
    exploded_detail_events
    .select(
        "*",
        F.last("requester_id").over(w_agent_id_event_id_by_agent_h).alias("last_requester_id"),
        F.row_number().over(w_agent_id_event_id_by_agent_h_2).alias("rn"),
        F.ntile(5).over(w_agent_id_event_id_by_agent_h_2).alias("agent_hired_on_quintile"),
        F.lead("agent_hired_on", 2).over(w_agent_id_event_id_by_agent_h_2).alias("next2_agent_hired_on"),
    )
)

result = (
    metrics
    .filter(F.col("next2_agent_hired_on").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "channel_clean",
        "agent_hired_on",
        "agent_team_2",
        "event_id",
        "event_type",
        "event_actor",
        "rn",
        "agent_hired_on_quintile",
        "next2_agent_hired_on",
    )
    .orderBy(F.col("event_id").desc_nulls_first())
)
