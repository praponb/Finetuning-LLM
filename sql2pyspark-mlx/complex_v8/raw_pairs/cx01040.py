from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority_by_agent_h = Window.partitionBy("agent_id", "priority").orderBy(F.col("agent_hired_on").desc(), "id", "tag")

filtered = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

with_parent_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", "su.agent_name", F.col("su.hired_on").alias("agent_hired_on"))
)

flattened = (
    with_parent_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "greatest_csat_requester_id",
        "agent_name",
        "agent_hired_on",
        F.explode("tags").alias("tag"),
    )
)

enriched = (
    flattened.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("J%")), "inner")
    .filter(F.col("su.team").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.agent_hired_on",
        "s.tag",
        F.col("su.team").alias("agent_team"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(5).over(w_agent_id_priority_by_agent_h).alias("agent_hired_on_quintile"),
        F.row_number().over(w_agent_id_priority_by_agent_h).alias("agent_hired_on_seq"),
        F.ntile(4).over(w_agent_id_priority_by_agent_h).alias("agent_hired_on_quartile"),
    )
)

with_parent_agent_2 = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.agent_hired_on",
        "s.tag",
        "s.agent_agent_name",
        "s.agent_hired_on_quartile",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.team").alias("agent_team_2"),
    )
)

result = (
    with_parent_agent_2
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "priority",
        "agent_hired_on",
        "tag",
        "agent_hired_on_quartile",
        "agent_tier",
        "agent_hired_on_2",
        "agent_team_2",
        F.length(F.trim(F.col("agent_team_2"))).alias("agent_team_2_len"),
        F.quarter(F.col("agent_hired_on")).alias("agent_hired_on_quarter"),
    )
    .orderBy("tag", F.col("agent_hired_on").asc(), "id")
    .limit(20)
)
