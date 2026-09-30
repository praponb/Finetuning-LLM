from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_csat_by_requester_id = Window.partitionBy("csat").orderBy("requester_id", "id")
w_channel_agent_id_by_agent_hi = Window.partitionBy("channel", "agent_id").orderBy(F.col("agent_hired_on").desc(), "id")

recent = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(10, 14)) & (F.col("requester_id") >= 7))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
    )
)

priority_stats = (
    recent
    .groupBy("priority")
    .agg(F.max("csat").alias("max_csat"))
)

compared = (
    recent.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

enriched_agent = (
    compared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.max_csat",
        "s.csat_minus_max",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    enriched_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "csat_minus_max",
        "agent_name",
        "agent_hired_on",
        F.round(F.avg("max_csat").over(w_channel), 2).alias("channel_avg_max_csat"),
        F.lead("closed_at").over(w_csat_by_requester_id).alias("next_closed_at"),
        F.row_number().over(w_channel_agent_id_by_agent_hi).alias("rn"),
    )
)

result = (
    sequenced
    .rollup("channel", "csat", "agent_id")
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.min("csat").alias("min_csat"),
        F.count("*").alias("cnt"),
    )
)
