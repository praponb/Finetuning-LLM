from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_team").orderBy(F.col("closed_at").desc(), F.col("id").desc())

cleaned = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-28 19:00:00").cast("timestamp"))
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

agent_id_stats = (
    cleaned
    .groupBy("agent_id")
    .agg(F.max("csat").alias("max_csat"))
)

vs_group = (
    cleaned.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

enriched_agent = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Tech"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

latest = (
    enriched_agent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["priority", "agent_team", "channel"], ["priority"], ["agent_team"], []],
        "priority", "agent_team", "channel",
    )
    .agg(
        F.sum(F.when(~F.col("channel").isin("email", "chat"), F.col("csat"))).alias("csat_channel_not_in_email_chat"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "csat_channel_not_in_email_chat",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
