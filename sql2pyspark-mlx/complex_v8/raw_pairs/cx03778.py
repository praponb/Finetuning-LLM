from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_tier", "priority").orderBy("opened_at", "id")

prepared = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(9, 13)) | (F.col("requester_id") < 22))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

filtered = (
    prepared
    .crossJoin(prepared.agg(F.avg("requester_id").alias("overall_avg_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_avg_requester_id"))
    .drop("overall_avg_requester_id")
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        F.col("su.team").alias("agent_team"),
    )
)

joined_agent = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.agent_team",
        "su.agent_name",
    )
)

joined = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.tier").alias("agent_tier"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    latest
    .select(
        "*",
        F.coalesce(F.col("agent_team_2"), F.lit("UNKNOWN")).alias("agent_team_2_or_unknown"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
    )
)

result = (
    derived
    .select(
        "id",
        "opened_at",
        "priority",
        "agent_team",
        "agent_name",
        "agent_team_2",
        "agent_agent_name",
        "agent_tier",
        "opened_at_plus_2h",
    )
    .orderBy("agent_name")
)
