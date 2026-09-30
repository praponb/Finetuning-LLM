from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())

filtered = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) | (F.col("priority").isin("P2", "P3", "P1")))
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
    )
)

latest_per_key = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_agent = (
    latest_per_key.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

joined_agent_2 = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.agent_team",
        "s.agent_name",
        "s.agent_tier",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team_2"),
    )
)

result = (
    joined_agent_2
    .rollup("csat", "agent_team_2", "agent_tier")
    .agg(
        F.grouping("csat").alias("is_total_csat"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.round(100 * F.count(F.when(F.col("channel") == "email", 1)) / F.count("*"), 1).alias("pct_channel_email"),
        F.sum("requester_id").alias("total_requester_id"),
    )
    .select(
        "csat",
        F.coalesce(F.col("agent_team_2"), F.lit("ALL")).alias("agent_team_2"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "is_total_csat",
        "requester_id_to_csat_ratio",
        "pct_channel_email",
        "total_requester_id",
    )
)
