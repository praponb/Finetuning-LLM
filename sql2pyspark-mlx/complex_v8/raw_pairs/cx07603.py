from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tag_pos_by_last_event_at = Window.partitionBy("tag_pos").orderBy("last_event_at", "id", "tag_pos")
w_tag_pos_by_last_event_at_tie = Window.partitionBy("tag_pos").orderBy("last_event_at")
w_csat_csat_filled = Window.partitionBy("csat", "csat_filled")

base = (
    df_tickets
    .filter((F.col("csat") > 3) & (F.col("requester_id") >= 4))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

exploded = (
    base
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "csat_filled",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

with_parent_agent = (
    exploded.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.csat_filled",
        "s.tag_pos",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

joined = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 4)
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.csat_filled",
        "s.tag_pos",
        "s.agent_hired_on",
        "s.agent_team",
        "s.agent_name",
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type").isin("created", "replied"), 1)).alias("event_event_type_count"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

with_trends = (
    profiled
    .select(
        "*",
        F.ntile(3).over(w_tag_pos_by_last_event_at).alias("last_event_at_tercile"),
        F.rank().over(w_tag_pos_by_last_event_at_tie).alias("last_event_at_rank"),
        F.round(F.col("event_event_type_count") - F.avg("event_event_type_count").over(w_csat_csat_filled), 2).alias("event_event_type_count_vs_csat_csat_filled_avg"),
        F.count(F.lit(1)).over(w_csat_csat_filled).alias("rows_in_csat_csat_filled"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "agent_id",
        "csat",
        "csat_filled",
        "tag_pos",
        "agent_name",
        "agent_agent_name",
        "event_event_type_count",
        "event_actor_count",
        "last_event_at",
        "last_event_at_tercile",
        "event_event_type_count_vs_csat_csat_filled_avg",
        "rows_in_csat_csat_filled",
    )
    .orderBy(F.col("agent_agent_name").asc())
)
